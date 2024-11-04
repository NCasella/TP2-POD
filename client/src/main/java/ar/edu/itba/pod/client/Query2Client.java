package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.YDTPerAgencyCollator;
import ar.edu.itba.pod.combiners.YDTPerAgencyMapperCombinerFactory;
import ar.edu.itba.pod.mappers.YDTPerAgencyMapper;
import ar.edu.itba.pod.models.MoneyRaisedPerMonth;
import ar.edu.itba.pod.models.YearAgencyKey;
import ar.edu.itba.pod.reducers.YDTPerAgencyReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Query2Client extends AbstractClient{
    private final String idMap = LocalDateTime.now().toString();
    private final static int MONTHS = 12;

    @Override
    protected void runClientCode() {

        // Key Value Source
        IMap<Integer,String> imap1 = hazelcastInstance.getMap("YDTPerAgency" + idMap);

        // Job Tracker
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query2"+ idMap);

        System.out.println("-------- READING FILES --------");
        System.out.println(LocalDateTime.now());

        final AtomicInteger auxKey = new AtomicInteger();
        try  {
            Stream<String> lines = Files.lines(Paths.get(inPath+"tickets"+cityParam+".csv"), StandardCharsets.UTF_8);
            lines = lines.skip(1);
            lines.forEach(line -> imap1.put(auxKey.getAndIncrement(), line));

            Set<String> agencies = new HashSet<>();
            lines= Files.lines(Paths.get(inPath+"/agencies"+cityParam+".csv"));
            lines.skip(1).forEach(agencies::add);


            System.out.println(LocalDateTime.now());
            KeyValueSource<Integer,String> YDTPerAgencyKeyValueSource = KeyValueSource.fromMap(imap1);
            Job<Integer,String> jobYDTPerAgency = jobTracker.newJob(YDTPerAgencyKeyValueSource);

             ICompletableFuture<List<Map.Entry<YearAgencyKey,MoneyRaisedPerMonth>>> future = jobYDTPerAgency
                                .mapper(new YDTPerAgencyMapper(cityParam,agencies))
                                .combiner(new YDTPerAgencyMapperCombinerFactory())
                                .reducer(new YDTPerAgencyReducerFactory())
                                .submit(new YDTPerAgencyCollator());

            List<Map.Entry<YearAgencyKey,MoneyRaisedPerMonth>> result = future.get();
            System.out.println(LocalDateTime.now());

            System.out.println("TOTAL: "+result.size());


            try {
                Path path= Paths.get(outPath+"/query2.csv");
                Files.write(path,"Agency;Year;Month;YTD\n".getBytes());

                for( Map.Entry<YearAgencyKey,MoneyRaisedPerMonth> e : result){
                    StringBuilder s=new StringBuilder();
                    String agencyYear = e.getKey().getAgency() + ";" + e.getKey().getYear() + ";";
                    long[] moneyRaisedPerMonth = e.getValue().getMoneyRaisedPerMonth();
                    for ( int i=0; i<MONTHS ; i++ ) {
                        if ( e.getValue().monthRaisedMoneyYDT(i) )
                            s.append(agencyYear).append(i+1).append(";").append(moneyRaisedPerMonth[i]).append("\n");
                    }
                    Files.write(path,s.toString().getBytes(), StandardOpenOption.APPEND);
                }

            } catch (InvalidPathException | NoSuchFileException e) {
                System.out.println("Invalid path, query2.csv won't be created");
            }


        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            imap1.destroy();
            System.out.println("fin");
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new Query2Client().clientMain();
    }
}
