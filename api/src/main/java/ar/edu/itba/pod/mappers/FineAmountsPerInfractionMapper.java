package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class FineAmountsPerInfractionMapper implements Mapper<String, Ticket, String, Integer> {

    @Override
    public void map(String agency, Ticket ticket, Context<String, Integer> context){
       context.emit(ticket.getInfractionId(), ticket.getFineAmount());
    }
}
