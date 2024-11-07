package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.ReincidentPlatesPerNeighbourhoodCollator;
import ar.edu.itba.pod.combiners.ReincidentPlatesInNeighbourhoodByInfractionTypeCombinerFactory;
import ar.edu.itba.pod.combiners.ReincidentPlatesInNeighbourhoodCombinerFactory;
import ar.edu.itba.pod.combiners.ReincidentPlatesPerNeighbourhoodCombinerFactory;
import ar.edu.itba.pod.exceptions.InvalidFilePathException;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Query3Client extends AbstractClient{
    private LocalDate fromDateParam;
    private LocalDate toDateParam;
    private static final String DATE_PATTERN = "dd/MM/yyyy";
    private static final int MIN_N = 2;
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN);
    private Integer nParam;

    public Query3Client(){this.queryNumber=3;}

    @Override
    protected void getParams() {
        super.getParams();
        nParam = getNParam(MIN_N);
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
    protected void runClientCode() throws IOException, ExecutionException, InterruptedException{
        Logger logger = LoggerFactory.getLogger(Query3Client.class);

        IMap<Integer, String> imap1 = hazelcastInstance.getMap("g7-ReincidentPlates" + idMap);
        IMap<PlateInfractionInNeighbourhood,Integer> imap2 = hazelcastInstance.getMap("g7-ReincidentPlates2" + idMap);
        IMap<PlateInNeighbourhood,Boolean> imap3 = hazelcastInstance.getMap("g7-ReincidentPlates3" + idMap);

        distributedCollections = Arrays.asList(imap1, imap2, imap3);

        KeyValueSource<Integer, String> reincidentPlatesKeyValueSource = KeyValueSource.fromMap(imap1);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("g7-reincidentPlates-count"+ idMap);

        final AtomicInteger auxKey = new AtomicInteger();
        try (Stream<String> lines = Files.lines(Paths.get(inPath+"/tickets"+cityParam+".csv"), StandardCharsets.UTF_8).parallel()) {
            logger.info("Inicio de lectura de archivos de entrada");
            lines.skip(1).forEach(line -> imap1.put(auxKey.getAndIncrement(), line));
        }
            logger.info("Fin de lectura de archivos de entrada");

            // ---------------------------------------------------- JOB 1 ---------------------------------------------------- //

            logger.info("Inicio del trabajo map/reduce 1");
            Job<Integer, String> jobPlatesInNeighbourhoodByInfractionType = jobTracker.newJob(reincidentPlatesKeyValueSource);

            ICompletableFuture<Map<PlateInfractionInNeighbourhood,Integer>> future = jobPlatesInNeighbourhoodByInfractionType
                    .mapper(new ReincidentPlatesInNeighbourhoodByInfractionTypeMapper(fromDateParam, toDateParam, cityParam))
                    .combiner(new ReincidentPlatesInNeighbourhoodByInfractionTypeCombinerFactory())
                    .reducer(new ReincidentPlatesInNeighbourhoodByInfractionTypeReducerFactory())
                    .submit();

            Map<PlateInfractionInNeighbourhood, Integer> result = future.get();
            logger.info("Fin map/reduce 1");

            // ---------------------------------------------------- JOB 2 ---------------------------------------------------- //

            imap2.putAll(result);
            logger.info("Inicio del trabajo map/reduce 2");

            KeyValueSource<PlateInfractionInNeighbourhood,Integer> reincidentPlatesInNeightbourhoodKeyValueSource = KeyValueSource.fromMap(imap2);
            Job<PlateInfractionInNeighbourhood,Integer> jobPlatesInNeighbourhood = jobTracker.newJob(reincidentPlatesInNeightbourhoodKeyValueSource);

            ICompletableFuture<Map<PlateInNeighbourhood,Boolean>> future2 = jobPlatesInNeighbourhood
                    .mapper(new ReincidentPlatesInNeighbourhoodMapper(nParam))
                    .combiner(new ReincidentPlatesInNeighbourhoodCombinerFactory())
                    .reducer(new ReincidentPlatesInNeighbourhoodReducerFactory())
                    .submit();

            Map<PlateInNeighbourhood, Boolean> result2 = future2.get();
            logger.info("Fin map/reduce 2");

            // ---------------------------------------------------- JOB 3 ---------------------------------------------------- //

            imap3.putAll(result2);
            logger.info("Inicio del trabajo map/reduce 3");

            KeyValueSource<PlateInNeighbourhood,Boolean> reincidentPlatesPerNeightbourhoodKeyValueSource = KeyValueSource.fromMap(imap3);
            Job<PlateInNeighbourhood,Boolean> jobPlatesPerNeighbourhood = jobTracker.newJob(reincidentPlatesPerNeightbourhoodKeyValueSource);

            ICompletableFuture<List<Map.Entry<String,Double>>> future3 = jobPlatesPerNeighbourhood
                    .mapper(new ReincidentPlatesPerNeighbourhoodMapper() )
                    .combiner(new ReincidentPlatesPerNeighbourhoodCombinerFactory())
                    .reducer(new ReincidentPlatesPerNeighbourhoodReducerFactory())
                    .submit(new ReincidentPlatesPerNeighbourhoodCollator());

            List<Map.Entry<String,Double>> result3 = future3.get();

            logger.info("Fin map/reduce 3");
            logger.info("Comienza escritura");

            try {
                Path path= Paths.get(outPath+"/query3.csv");
                Files.write(path,"County;Percentage\n".getBytes());

                for( Map.Entry<String,Double> e : result3){
                    StringBuilder stringToWrite=new StringBuilder(e.getKey())
                            .append(";").append(e.getValue().toString().formatted("%.2f%%")).append("%\n");
                    Files.write(path,stringToWrite.toString().getBytes(), StandardOpenOption.APPEND);
                }
                logger.info("Fin escritura\n");
            } catch (InvalidPathException | NoSuchFileException e) {
                throw new InvalidFilePathException(e.getMessage());
            }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new Query3Client().clientMain();
    }
}
