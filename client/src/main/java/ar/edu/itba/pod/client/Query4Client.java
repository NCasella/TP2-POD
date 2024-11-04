package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.FineAmountsPerInfractionCollator;
import ar.edu.itba.pod.collators.TotalFinesPerInfractionAndAgencyCollator;

import ar.edu.itba.pod.mappers.FineAmountsPerInfractionMapper;
import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.InfractionAgencyKey;
import ar.edu.itba.pod.models.InfractionFinesDifferences;
import ar.edu.itba.pod.models.Ticket;
import ar.edu.itba.pod.reducers.FineAmountsPerInfractionReducerFactory;
import ar.edu.itba.pod.reducers.TotalFinesPerInfractionAndAgencyReducer;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;


public class Query4Client extends AbstractClient{
    private Integer nParam;
    Logger logger= LoggerFactory.getLogger(Query4Client.class);

    protected void runClientCode() throws IOException, ExecutionException,InterruptedException {
        //unificar chequeos
        if (System.getProperty("city") == null){
            throw new IllegalArgumentException("City is needed");
        }else if (!(System.getProperty("city").equals("CHI") || System.getProperty("city").equals("NYC"))){
            throw new IllegalArgumentException("Invalid city");
        }
        if ( System.getProperty("n") == null || Integer.parseInt(System.getProperty("n")) < 1){
            throw new IllegalArgumentException("n is invalid");
        }
        if ( System.getProperty("agency") == null ){
            throw new IllegalArgumentException("agency is needed");
        }
        nParam=Integer.parseInt(System.getProperty("n"));
        String agencyName = System.getProperty("agency");

        ISet<String> agenciesISet = hazelcastInstance.getSet("agencyNames");
        IMap<String, Infraction> infractionIMap = hazelcastInstance.getMap("infractionsById");
        IMap<Integer, Ticket> ticketsIMap=hazelcastInstance.getMap("ticketsByAgency");

        try {

            Stream<String> lines = Files.lines(Paths.get(inPath + "/agencies" + cityParam + ".csv"));
            lines.skip(1).forEach(agenciesISet::add);

            if (!agenciesISet.contains(agencyName)) {
                throw new IllegalArgumentException("Invalid agency");
            }
            lines = Files.lines(Paths.get(inPath + "/infractions" + cityParam + ".csv"));
            lines.skip(1).forEach(line -> {
                    String[] fields = line.split(";");
                    infractionIMap.put(fields[0], new Infraction(fields[0], fields[1]));
            });
            final AtomicInteger auxKey = new AtomicInteger();
            lines = Files.lines(Paths.get(inPath + "/tickets" + cityParam + ".csv"));
            lines.skip(1).forEach(line -> {
                String[] fields = line.split(";");
                Ticket ticket = cityParam.getTicket(fields);
                if ( infractionIMap.containsKey(ticket.getInfractionId())) {
                    logger.info(infractionIMap.getEntryView(ticket.getInfractionId()).getValue().getDescription()+ '-' + ticket.getAgencyName());
                    ticket.setInfractionId(infractionIMap.get(ticket.getInfractionId()).getDescription());
                    ticketsIMap.put(auxKey.getAndIncrement(),ticket);
                }
            });

            JobTracker jobTracker = hazelcastInstance.getJobTracker("getMaxDiffPerInfraction");
            KeyValueSource<Integer, Ticket> source = KeyValueSource.fromMap(ticketsIMap);
            Job<Integer, Ticket> job = jobTracker.newJob(source);

            logger.info("Inicio del trabajo map/reduce");
            List<Map.Entry<String, InfractionFinesDifferences>> result = job
                    .mapper(new FineAmountsPerInfractionMapper(agencyName))
                    .reducer(new FineAmountsPerInfractionReducerFactory())
                    .submit(new FineAmountsPerInfractionCollator(nParam)).get();

            try {
                Path path = Paths.get(outPath + "/query4.csv");
                Files.write(path, "Infraction;Min;Max;Diff\n".getBytes());
                for (Map.Entry<String, InfractionFinesDifferences> entry : result) {
                    Files.write(path, entry.getValue().toString().getBytes(), StandardOpenOption.APPEND);
                }
            } catch (InvalidPathException | NoSuchFileException e) {
                System.out.println("Invalid path, query3.csv won't be created");
            }
        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }finally {
            agenciesISet.destroy();
            infractionIMap.destroy();
            ticketsIMap.destroy();
        }
        logger.info("Fin del trabajo map/reduce");
    }
    public static void main(String[] args) throws IOException, ExecutionException,InterruptedException {
        new Query4Client().clientMain();
    }

}
