package ar.edu.itba.pod.client;

import ar.edu.itba.pod.combiners.ReincidentPlatesPerNeighbourhoodCombinerFactory;
import ar.edu.itba.pod.mappers.ReincidentPlatesPerNeighbourhoodMapper;
import ar.edu.itba.pod.models.PlateInNeighbourhood;
import ar.edu.itba.pod.reducers.ReincidentPlatesPerNeighbourhoodReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ReincidentPlatesPerNeighbourhoodClient extends AbstractClient{

    public ReincidentPlatesPerNeighbourhoodClient() {
        dateTimeFormatter =  DateTimeFormatter.ofPattern("dd/MM/yyyy");
    }

    @Override
    protected void runClientCode() {

        // Key Value Source
        IMap<Integer, String> reincidentPlatesIMap = hazelcastInstance.getMap("ReincidentPlates");
        KeyValueSource<Integer, String> reincidentPlatesKeyValueSource = KeyValueSource.fromMap(reincidentPlatesIMap);

        // Job Tracker
        JobTracker jobTracker = hazelcastInstance.getJobTracker("reincidentPlates-count");

        // Text File Reading and Key Value Source Loading
        final AtomicInteger auxKey = new AtomicInteger();
        try  {
            Stream<String> lines = Files.lines(Path.of(inPath), StandardCharsets.UTF_8);
            lines = lines.skip(1);
            lines.forEach(line -> reincidentPlatesIMap.put(auxKey.getAndIncrement(), line));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // MapReduce Job
        Job<Integer, String> job = jobTracker.newJob(reincidentPlatesKeyValueSource);

        ICompletableFuture<Map<PlateInNeighbourhood,Integer>> future = job
                .mapper(new ReincidentPlatesPerNeighbourhoodMapper(fromDateParam, toDateParam, cityParam))
                .combiner(new ReincidentPlatesPerNeighbourhoodCombinerFactory())
                .reducer(new ReincidentPlatesPerNeighbourhoodReducerFactory())
                .submit();

        // Wait and retrieve the result
        try {
            Map<PlateInNeighbourhood, Integer> result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Sort entries ascending by count and print

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new ReincidentPlatesPerNeighbourhoodClient().clientMain();
    }
}
