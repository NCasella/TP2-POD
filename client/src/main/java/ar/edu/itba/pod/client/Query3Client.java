package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.ReincidentPlatesPerNeighbourhoodCollator;
import ar.edu.itba.pod.combiners.ReincidentPlatesInNeighbourhoodByInfractionTypeCombinerFactory;
import ar.edu.itba.pod.combiners.ReincidentPlatesInNeighbourhoodCombinerFactory;
import ar.edu.itba.pod.combiners.ReincidentPlatesPerNeighbourhoodCombinerFactory;
import ar.edu.itba.pod.exceptions.InvalidParamException;
import ar.edu.itba.pod.mappers.ReincidentPlatesInNeighbourhoodByInfractionTypeMapper;
import ar.edu.itba.pod.mappers.ReincidentPlatesInNeighbourhoodMapper;
import ar.edu.itba.pod.mappers.ReincidentPlatesPerNeighbourhoodMapper;
import ar.edu.itba.pod.models.PlateInNeighbourhood;
import ar.edu.itba.pod.models.PlateInfractionInNeighbourhood;
import ar.edu.itba.pod.reducers.ReincidentPlatesInNeighbourhoodByInfractionTypeReducerFactory;
import ar.edu.itba.pod.reducers.ReincidentPlatesInNeighbourhoodReducerFactory;
import ar.edu.itba.pod.reducers.ReincidentPlatesPerNeighbourhoodReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Query3Client extends AbstractClient{
    private LocalDate fromDateParam;
    private LocalDate toDateParam;
    private static final String DATE_PATTERN = "dd/MM/yyyy";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN);
    private static final DecimalFormat df = new DecimalFormat("#.##");
    private Integer nParam;
    private final String idMap = LocalDateTime.now().toString();
    Logger logger= LoggerFactory.getLogger(Query3Client.class);

    private void getParams() {
        try {
            nParam=Integer.parseInt(System.getProperty("n"));
        } catch (NumberFormatException e) {
            throw new InvalidParamException("'n' param as integer is required");
        }
        if ( nParam<2 )
            throw new InvalidParamException("'n' param can't be less than 2");
        try {
            fromDateParam= LocalDate.parse(System.getProperty("from"),dateTimeFormatter);
        } catch (DateTimeParseException e) {
            throw new InvalidParamException("'from' date param as '" + DATE_PATTERN + "' is required");
        }
        try {
            toDateParam=LocalDate.parse(System.getProperty("to"),dateTimeFormatter);
        } catch (DateTimeParseException e) {
            throw new InvalidParamException("'to' date param as '" + DATE_PATTERN + "' is required");
        }
        if ( toDateParam.isBefore(fromDateParam) )
            throw new InvalidParamException("'to' date param should be after 'from' date");
    }

    @Override
    protected void runClientCode() {
        getParams();

        // Key Value Source
        IMap<Integer, String> imap1 = hazelcastInstance.getMap("ReincidentPlates" + idMap);
        KeyValueSource<Integer, String> reincidentPlatesKeyValueSource = KeyValueSource.fromMap(imap1);
        IMap<PlateInfractionInNeighbourhood,Integer> imap2 = hazelcastInstance.getMap("ReincidentPlates2" + idMap);
        IMap<PlateInNeighbourhood,Boolean> imap3 = hazelcastInstance.getMap("ReincidentPlates3" + idMap);

        // Job Tracker
        JobTracker jobTracker = hazelcastInstance.getJobTracker("reincidentPlates-count"+ idMap);

        System.out.println("-------- READING FILE --------");
        System.out.println(LocalDateTime.now());

        final AtomicInteger auxKey = new AtomicInteger();
        try  {

            logger.info("Inicio de lectura de archivos de entrada");
            Stream<String> lines = Files.lines(Paths.get(inPath+"/tickets"+cityParam+".csv"), StandardCharsets.UTF_8);
            lines = lines.skip(1);
            lines.forEach(line -> imap1.put(auxKey.getAndIncrement(), line));
            logger.info("Fin de lectura de archivos de entrada");

            // ---------------------------------------------------- JOB 1 ---------------------------------------------------- //
            System.out.println("-------- JOB 1 --------");
            System.out.println(LocalDateTime.now());

            logger.info("Inicio del trabajo map/reduce 1");
            // MapReduce Job
            Job<Integer, String> jobPlatesInNeighbourhoodByInfractionType = jobTracker.newJob(reincidentPlatesKeyValueSource);

            ICompletableFuture<Map<PlateInfractionInNeighbourhood,Integer>> future = jobPlatesInNeighbourhoodByInfractionType
                    .mapper(new ReincidentPlatesInNeighbourhoodByInfractionTypeMapper(fromDateParam, toDateParam, cityParam))
                    .combiner(new ReincidentPlatesInNeighbourhoodByInfractionTypeCombinerFactory())
                    .reducer(new ReincidentPlatesInNeighbourhoodByInfractionTypeReducerFactory())
                    .submit();

            Map<PlateInfractionInNeighbourhood, Integer> result = future.get();
            System.out.println(LocalDateTime.now());

            //result.forEach(
            //        (k, v) -> System.out.println(k + ": " + v)
            //);
            System.out.println("TOTAL: "+result.size());
            logger.info("Fin map/reduce 1\n");
            // ---------------------------------------------------- JOB 2 ---------------------------------------------------- //
            System.out.println("-------- JOB 2 --------");
            imap2.putAll(result);
            System.out.println(LocalDateTime.now());
            logger.info("Inicio del trabajo map/reduce 2");

            KeyValueSource<PlateInfractionInNeighbourhood,Integer> reincidentPlatesInNeightbourhoodKeyValueSource = KeyValueSource.fromMap(imap2);
            Job<PlateInfractionInNeighbourhood,Integer> jobPlatesInNeighbourhood = jobTracker.newJob(reincidentPlatesInNeightbourhoodKeyValueSource);

            ICompletableFuture<Map<PlateInNeighbourhood,Boolean>> future2 = jobPlatesInNeighbourhood
                    .mapper(new ReincidentPlatesInNeighbourhoodMapper(nParam))
                    .combiner(new ReincidentPlatesInNeighbourhoodCombinerFactory())
                    .reducer(new ReincidentPlatesInNeighbourhoodReducerFactory())
                    .submit();

            Map<PlateInNeighbourhood, Boolean> result2 = future2.get();
            System.out.println(LocalDateTime.now());

            //result.forEach(
            //        (k, v) -> System.out.println(k + ": " + v)
            //);
            System.out.println("TOTAL: "+result2.size());
            logger.info("Fin map/reduce 2\n");
            // ---------------------------------------------------- JOB 3 ---------------------------------------------------- //
            System.out.println("-------- JOB 3 --------");
            System.out.println(LocalDateTime.now());
            imap3.putAll(result2);
            logger.info("Inicio del trabajo map/reduce 3");

            KeyValueSource<PlateInNeighbourhood,Boolean> reincidentPlatesPerNeightbourhoodKeyValueSource = KeyValueSource.fromMap(imap3);
            Job<PlateInNeighbourhood,Boolean> jobPlatesPerNeighbourhood = jobTracker.newJob(reincidentPlatesPerNeightbourhoodKeyValueSource);

            ICompletableFuture<List<Map.Entry<String,Double>>> future3 = jobPlatesPerNeighbourhood
                    .mapper(new ReincidentPlatesPerNeighbourhoodMapper() )
                    .combiner(new ReincidentPlatesPerNeighbourhoodCombinerFactory())
                    .reducer(new ReincidentPlatesPerNeighbourhoodReducerFactory())
                    .submit(new ReincidentPlatesPerNeighbourhoodCollator());

            // Wait and retrieve the result
            List<Map.Entry<String,Double>> result3 = future3.get();

            System.out.println(LocalDateTime.now());
            // result3.forEach( e -> System.out.println(e.getKey() + ": " + e.getValue()));
            System.out.println("TOTAL: "+result3.size());
            logger.info("Fin map/reduce 3\n");

            logger.info("Comienza escritura");

            try {
                Path path= Paths.get(outPath+"/query3.csv");
                Files.write(path,"County;Percentage\n".getBytes());

                for( Map.Entry<String,Double> e : result3){
                    StringBuilder stringToWrite=new StringBuilder(e.getKey())
                            .append(";").append(e.getValue().toString().formatted("%.2f%%")).append('%').append("\n");
                    Files.write(path,stringToWrite.toString().getBytes(), StandardOpenOption.APPEND);
                }

            } catch (InvalidPathException | NoSuchFileException e) {
                System.out.println("Invalid path, query3.csv won't be created");
            }


        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            imap1.destroy();
            imap2.destroy();
            imap3.destroy();
            System.out.println("fin");
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new Query3Client().clientMain();
    }
}
