package ar.edu.itba.pod.client;

import ar.edu.itba.pod.Collators.TotalFinesPerInfractionAndAgencyCollator;
import ar.edu.itba.pod.combiners.TotalFinesPerInfractionAndAgencyCombiner;
import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.InfractionAgencyKey;
import ar.edu.itba.pod.models.Ticket;
import ar.edu.itba.pod.mappers.TotalFinesPerInfractionAndAgencyMapper;
import ar.edu.itba.pod.reducer.TotalFinesPerInfractionAndAgencyReducer;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import javax.xml.crypto.dsig.keyinfo.KeyValue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.Key;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class Query1Client extends AbstractClient{

    @Override
    protected void runClientCode() throws IOException,ExecutionException,InterruptedException{
        IMap<String, Ticket> ticketsMap=hazelcastInstance.getMap("g7-tickets");
        IMap<String, Infraction> violationsMap= hazelcastInstance.getMap("g7-violations");

        try(Stream<String> lines= Files.lines(Paths.get(inPath+"/ticketsMini"+cityParam+".csv"))){
            lines.skip(1).forEach(line->{
                String[] fields=line.split(";");
                ticketsMap.put(fields[cityParam.getInfractionIdIndex()],cityParam.getTicket(fields));
            });
        }
        try(Stream<String> lines=Files.lines(Paths.get(inPath+"/infractions"+cityParam+".csv"))){
            lines.skip(1).forEach(line->{
                String[] fields=line.split(";");
                violationsMap.put(fields[0],new Infraction(fields[0],fields[1]));
            });
        }

        JobTracker jobTracker=hazelcastInstance.getJobTracker("g7-totalFinesPerInfractionAndAgency");
        KeyValueSource<String,Ticket> source=KeyValueSource.fromMap(ticketsMap);
        Job<String,Ticket> job= jobTracker.newJob(source);

        SortedSet<Map.Entry<InfractionAgencyKey,Long>> result=job
                .mapper(new TotalFinesPerInfractionAndAgencyMapper())
                .combiner(new TotalFinesPerInfractionAndAgencyCombiner())
                .reducer(new TotalFinesPerInfractionAndAgencyReducer())
                .submit(new TotalFinesPerInfractionAndAgencyCollator()).get();
        Path path=Paths.get(outPath+"/InfractionAndFinesOut.csv");
        Files.write(path,"Infraction;Agency;Tickets\n".getBytes());

        for(Map.Entry<InfractionAgencyKey,Long> entry:result){
            StringBuilder stringToWrite=new StringBuilder(entry.getKey().getInfraction())
                            .append(";").append(entry.getKey().getAgency()).append(";").append(entry.getValue()).append("\n");
            Files.write(path,stringToWrite.toString().getBytes(),StandardOpenOption.APPEND);
        }
    }

    public static void main(String[] args) throws IOException, ExecutionException,InterruptedException {
        new Query1Client().clientMain();
    }
}
