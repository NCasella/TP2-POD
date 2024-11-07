package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.YDTPerAgencyCollator;
import ar.edu.itba.pod.combiners.YDTPerAgencyMapperCombinerFactory;
import ar.edu.itba.pod.exceptions.InvalidFilePathException;
import ar.edu.itba.pod.mappers.YDTPerAgencyMapper;
import ar.edu.itba.pod.models.MoneyRaisedPerMonth;
import ar.edu.itba.pod.models.YearAgencyKey;
import ar.edu.itba.pod.reducers.YDTPerAgencyReducerFactory;
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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Query2Client extends AbstractClient{
    private final static int MONTHS = 12;

    public Query2Client(){this.queryNumber=2;}

    @Override
    protected void runClientCode() throws IOException,ExecutionException,InterruptedException {
        Logger logger=LoggerFactory.getLogger(Query2Client.class);

        IMap<Integer,String> imap1 = hazelcastInstance.getMap("YDTPerAgency" + idMap);
        distributedCollections = Collections.singletonList(imap1);

        JobTracker jobTracker = hazelcastInstance.getJobTracker("query2"+ idMap);

        logger.info("Inicio de lectura de archivos de entrada");

        final AtomicInteger auxKey = new AtomicInteger();
        try(Stream<String> lines = Files.lines(Paths.get(inPath+"/tickets"+cityParam+".csv"), StandardCharsets.UTF_8).parallel();) {
            lines.skip(1).forEach(line -> imap1.put(auxKey.getAndIncrement(), line));
        }
        Set<String> agencies = new HashSet<>();
        try(Stream<String> lines = Files.lines(Paths.get(inPath+"/agencies"+cityParam+".csv"))){
            lines.skip(1).forEach(agencies::add);
        }

        logger.info("Fin de lectura de archivos de entrada");

        KeyValueSource<Integer,String> YDTPerAgencyKeyValueSource = KeyValueSource.fromMap(imap1);
        Job<Integer,String> jobYDTPerAgency = jobTracker.newJob(YDTPerAgencyKeyValueSource);

        logger.info("Inicio del trabajo map/reduce");
         ICompletableFuture<List<Map.Entry<YearAgencyKey,MoneyRaisedPerMonth>>> future = jobYDTPerAgency
                            .mapper(new YDTPerAgencyMapper(cityParam,agencies))
                            .combiner(new YDTPerAgencyMapperCombinerFactory())
                            .reducer(new YDTPerAgencyReducerFactory())
                            .submit(new YDTPerAgencyCollator());

        List<Map.Entry<YearAgencyKey,MoneyRaisedPerMonth>> result = future.get();

        logger.info("Fin map/reduce");
        logger.info("Comienza escritura");

        try {
            Path path= Paths.get(outPath+"/query2.csv");
            Files.write(path,"Agency;Year;Month;YTD\n".getBytes());

            for( Map.Entry<YearAgencyKey,MoneyRaisedPerMonth> e : result){
                StringBuilder s=new StringBuilder();
                String agencyYear = e.getKey().getAgency() + ";" + e.getKey().getYear() + ";";
                long[] moneyRaisedPerMonth = e.getValue().getMoneyRaisedPerMonth();
                for ( int i=0; i<MONTHS ; i++ ) {
                    if ( e.getValue().hasMonthRaisedMoneyYDT(i) )
                        s.append(agencyYear).append(i+1).append(";").append(moneyRaisedPerMonth[i]).append("\n");
                }
                Files.write(path,s.toString().getBytes(), StandardOpenOption.APPEND);
            }
            logger.info("Fin escritura\n");

        } catch (InvalidPathException | NoSuchFileException e) {
            throw new InvalidFilePathException(e.getMessage());
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new Query2Client().clientMain();
    }
}
