package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class FineAmountsPerInfractionMapper implements Mapper<Integer, Ticket, String, Integer> {
    private String agency;

    public FineAmountsPerInfractionMapper(String agencyName){
        this.agency = agencyName;
    }
    @Override
    public void map(Integer id, Ticket ticket, Context<String, Integer> context){
        if (!ticket.getAgencyName().equals(agency)){
            return;
        }
       context.emit(ticket.getInfractionId(), ticket.getFineAmount());
    }
}
