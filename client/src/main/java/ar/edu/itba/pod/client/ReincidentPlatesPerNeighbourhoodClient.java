package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.ReincidentPlatesPerNeighbourhoodCollator;
import ar.edu.itba.pod.combiners.ReincidentPlatesInNeighbourhoodCombinerFactory;
import ar.edu.itba.pod.combiners.ReincidentPlatesPerNeighbourhoodCombinerFactory;
import ar.edu.itba.pod.mappers.ReincidentPlatesInNeighbourhoodMapper;
import ar.edu.itba.pod.mappers.ReincidentPlatesPerNeighbourhoodMapper;
import ar.edu.itba.pod.models.PlateInNeighbourhood;
import ar.edu.itba.pod.reducers.ReincidentPlatesInNeighbourhoodReducerFactory;
import ar.edu.itba.pod.reducers.ReincidentPlatesPerNeighbourhoodReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ReincidentPlatesPerNeighbourhoodClient extends AbstractClient{
    private final static AtomicInteger lastId = new AtomicInteger();
    private LocalDate fromDateParam;
    private LocalDate toDateParam;
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DecimalFormat df = new DecimalFormat("#.##");
    private Integer nParam;
    private String idMap = LocalDate.now().toString();


    private void getParams() {
        if ( System.getProperty("from") == null )
            throw new IllegalArgumentException("from date is required");
        fromDateParam= LocalDate.parse(System.getProperty("from"),dateTimeFormatter);
        toDateParam=LocalDate.parse(System.getProperty("to"),dateTimeFormatter);
        nParam=Integer.parseInt(System.getProperty("n"));
    }

    @Override
    protected void runClientCode() {
        getParams();

        // Key Value Source
        IMap<Integer, String> reincidentPlatesIMap = hazelcastInstance.getMap("ReincidentPlates" + idMap);
        KeyValueSource<Integer, String> reincidentPlatesKeyValueSource = KeyValueSource.fromMap(reincidentPlatesIMap);
        IMap<PlateInNeighbourhood,Integer> imap2 = hazelcastInstance.getMap("ReincidentPlates2" + idMap);

        // Job Tracker
        JobTracker jobTracker = hazelcastInstance.getJobTracker("reincidentPlates-count"+ idMap);

        System.out.println("---- READING FILE ----");
        LocalDateTime startTime = LocalDateTime.now();
        System.out.println(startTime);
        final AtomicInteger auxKey = new AtomicInteger();
        try  {
            Stream<String> lines = Files.lines(Path.of(inPath), StandardCharsets.UTF_8);
            lines = lines.skip(1);
            lines.forEach(line -> reincidentPlatesIMap.put(auxKey.getAndIncrement(), line));

            LocalDateTime beforeJob1 = LocalDateTime.now();
            System.out.println("---- starting job ----");
            System.out.println(beforeJob1);

            // MapReduce Job
            Job<Integer, String> jobPlatesInNeighbourhood = jobTracker.newJob(reincidentPlatesKeyValueSource);

            ICompletableFuture<Map<PlateInNeighbourhood,Integer>> future = jobPlatesInNeighbourhood
                    .mapper(new ReincidentPlatesInNeighbourhoodMapper(fromDateParam, toDateParam, cityParam))
                    .combiner(new ReincidentPlatesInNeighbourhoodCombinerFactory())
                    .reducer(new ReincidentPlatesInNeighbourhoodReducerFactory())
                    .submit();

            // Wait and retrieve the result
            Map<PlateInNeighbourhood, Integer> result = future.get();
            System.out.println("Finished job 1");
            System.out.println(LocalDateTime.now());

            //result.forEach(
            //        (k, v) -> System.out.println(k + ": " + v)
            //);
            System.out.println("TOTAL: "+result.size());
            System.out.println(LocalDateTime.now());
            imap2.putAll(result);

            System.out.println("---- second job ----");
            System.out.println(LocalDateTime.now());
            KeyValueSource<PlateInNeighbourhood,Integer> reincidentPlatesPerNeightbourhoodKeyValueSource = KeyValueSource.fromMap(imap2);
            Job<PlateInNeighbourhood,Integer> jobPlatesPerNeighbourhood = jobTracker.newJob(reincidentPlatesPerNeightbourhoodKeyValueSource);

            ICompletableFuture<List<Map.Entry<String,Double>>> future2 = jobPlatesPerNeighbourhood
                    .mapper(new ReincidentPlatesPerNeighbourhoodMapper(nParam) )
                    .combiner(new ReincidentPlatesPerNeighbourhoodCombinerFactory())
                    .reducer(new ReincidentPlatesPerNeighbourhoodReducerFactory())
                    .submit(new ReincidentPlatesPerNeighbourhoodCollator());

            // Wait and retrieve the result
            List<Map.Entry<String,Double>> result2 = future2.get();
            System.out.println("Finished job2");
            System.out.println(LocalDateTime.now());
            // result2.forEach( e -> System.out.println(e.getKey() + ": " + e.getValue()));
            System.out.println("TOTAL: "+result2.size());

            try {
                Path path= Paths.get(outPath+"/query3.csv");
                Files.write(path,"County;Percentage\n".getBytes());

                for( Map.Entry<String,Double> e : result2){
                    StringBuilder stringToWrite=new StringBuilder(e.getKey())
                            .append(";").append( df.format(e.getValue()) ).append("\n");
                    Files.write(path,stringToWrite.toString().getBytes(), StandardOpenOption.APPEND);
                }

            } catch (InvalidPathException | NoSuchFileException e) {
                System.out.println("Invalid path, query3.csv won't be created");
            }


        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            reincidentPlatesIMap.destroy();
            imap2.destroy();
            System.out.println("fin");
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new ReincidentPlatesPerNeighbourhoodClient().clientMain();
    }
}
