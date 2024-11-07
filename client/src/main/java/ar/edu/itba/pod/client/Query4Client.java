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
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;


public class Query4Client extends AbstractClient {
    private Integer nParam;
    private Logger logger;

    public Query4Client() {
        this.queryNumber = 4;
    }

    @Override
    protected void runClientCode() throws IOException, ExecutionException, InterruptedException {
        logger = LoggerFactory.getLogger(Query4Client.class);
        if (System.getProperty("n") == null || Integer.parseInt(System.getProperty("n")) < 1) {
            System.out.println("n parameter is invalid");
            return;
        }
        if (System.getProperty("agency") == null) {
            System.out.println("agency not specified");
            return;
        }
        nParam = Integer.parseInt(System.getProperty("n"));
        String agencyName = System.getProperty("agency").replace("_", " ");

        ISet<String> agenciesISet = hazelcastInstance.getSet("agencyNames");
        IMap<String, Infraction> infractionIMap = hazelcastInstance.getMap("infractionsById");
        IMap<Integer, Ticket> ticketsIMap = hazelcastInstance.getMap("ticketsByAgency");
        logger.info("Inicio de lectura de archivos de entrada");

        try (Stream<String> lines = Files.lines(Paths.get(inPath + "/agencies" + cityParam + ".csv"))) {
            lines.skip(1).forEach(agenciesISet::add);
        }
        if (!agenciesISet.contains(agencyName)) {
                System.out.println("Invalid agency");
                return;
        }

        try (Stream<String> lines = Files.lines(Paths.get(inPath + "/infractions" + cityParam + ".csv"))) {
            lines.skip(1).forEach(line -> {
                String[] fields = line.split(";");
                infractionIMap.put(fields[0], new Infraction(fields[0], fields[1]));
            });
        }
        final AtomicInteger auxKey = new AtomicInteger();
        try (Stream<String> lines = Files.lines(Paths.get(inPath + "/tickets" + cityParam + ".csv")).parallel()) {
            lines.skip(1).forEach(line -> {
                String[] fields = line.split(";");
                Ticket ticket = cityParam.getTicket(fields);
                if (infractionIMap.containsKey(ticket.getInfractionId())) {
                    // logger.info(infractionIMap.getEntryView(ticket.getInfractionId()).getValue().getDescription()+ '-' + ticket.getAgencyName());
                    ticket.setInfractionId(infractionIMap.get(ticket.getInfractionId()).getDescription());
                    ticketsIMap.put(auxKey.getAndIncrement(), ticket);
                }
            });
        }
        logger.info("Fin de lectura de archivos de entrada");

        JobTracker jobTracker = hazelcastInstance.getJobTracker("getMaxDiffPerInfraction");
        KeyValueSource<Integer, Ticket> source = KeyValueSource.fromMap(ticketsIMap);
        Job<Integer, Ticket> job = jobTracker.newJob(source);


        logger.info("Inicio del trabajo map/reduce");
        List<Map.Entry<String, InfractionFinesDifferences>> result = job
                .mapper(new FineAmountsPerInfractionMapper(agencyName))
                .reducer(new FineAmountsPerInfractionReducerFactory())
                .submit(new FineAmountsPerInfractionCollator(nParam)).get();

        logger.info("Fin map/reduce");
        logger.info("Comienza escritura");
        try {
            Path path = Paths.get(outPath + "/query4.csv");
            Files.write(path, "Infraction;Min;Max;Diff\n".getBytes());
            for (Map.Entry<String, InfractionFinesDifferences> entry : result) {
                Files.write(path, entry.getValue().toString().getBytes(), StandardOpenOption.APPEND);
            }
        } catch (InvalidPathException | NoSuchFileException e) {
            System.out.println("Invalid path, query4.csv won't be created");
        }
        logger.info("Fin escritura\n");
        agenciesISet.destroy();
        infractionIMap.destroy();
        ticketsIMap.destroy();

    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        new Query4Client().clientMain();
    }
}

