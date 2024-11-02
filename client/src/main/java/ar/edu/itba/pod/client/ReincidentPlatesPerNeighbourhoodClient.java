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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ReincidentPlatesPerNeighbourhoodClient extends AbstractClient{
    private final static AtomicInteger idMap = new AtomicInteger();

    public ReincidentPlatesPerNeighbourhoodClient() {
        dateTimeFormatter =  DateTimeFormatter.ofPattern("dd/MM/yyyy");
    }

    @Override
    protected void runClientCode() {

        // Key Value Source
        IMap<Integer, String> reincidentPlatesIMap = hazelcastInstance.getMap("ReincidentPlates" + idMap.getAndIncrement());
        KeyValueSource<Integer, String> reincidentPlatesKeyValueSource = KeyValueSource.fromMap(reincidentPlatesIMap);
        IMap<PlateInNeighbourhood,Integer> imap2 = hazelcastInstance.getMap("ReincidentPlates2" + idMap.getAndIncrement());

        // Job Tracker
        JobTracker jobTracker = hazelcastInstance.getJobTracker("reincidentPlates-count");

        // Text File Reading and Key Value Source Loading
        final AtomicInteger auxKey = new AtomicInteger();
        try  {
            Stream<String> lines = Files.lines(Path.of(inPath), StandardCharsets.UTF_8);
            lines = lines.skip(1);
            lines.forEach(line -> reincidentPlatesIMap.put(auxKey.getAndIncrement(), line));

            System.out.println("Starting job");
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
            result.forEach(
                    (k, v) -> System.out.println(k + ": " + v)
            );
            System.out.println("TOTAL: "+result.size());

            imap2.putAll(result);

            KeyValueSource<PlateInNeighbourhood,Integer> reincidentPlatesPerNeightbourhoodKeyValueSource = KeyValueSource.fromMap(imap2);
            Job<PlateInNeighbourhood,Integer> jobPlatesPerNeighbourhood = jobTracker.newJob(reincidentPlatesPerNeightbourhoodKeyValueSource);

            ICompletableFuture<List<Map.Entry<String,Double>>> future2 = jobPlatesPerNeighbourhood
                    .mapper(new ReincidentPlatesPerNeighbourhoodMapper(nParam) )
                    .combiner(new ReincidentPlatesPerNeighbourhoodCombinerFactory())
                    .reducer(new ReincidentPlatesPerNeighbourhoodReducerFactory())
                    .submit(new ReincidentPlatesPerNeighbourhoodCollator());

            // Wait and retrieve the result
            List<Map.Entry<String,Double>> result2 = future2.get();
            System.out.println("Finished job");
            result2.forEach( e -> System.out.println(e.getKey() + ": " + e.getValue()));
            System.out.println("TOTAL: "+result2.size());

        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            reincidentPlatesIMap.destroy();
            imap2.destroy();
            System.out.println("fin");
            // Sort entries ascending by count and print
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new ReincidentPlatesPerNeighbourhoodClient().clientMain();
    }
}
