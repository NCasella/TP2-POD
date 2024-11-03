package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.InfractionAgencyKey;
import ar.edu.itba.pod.models.Ticket;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map;

public class TotalFinesPerInfractionAndAgencyMapper implements Mapper<String, Ticket, InfractionAgencyKey,Long>,HazelcastInstanceAware {

    private transient HazelcastInstance hazelcastInstance;
    @Override
    public void map(String s, Ticket ticket, Context<InfractionAgencyKey, Long> context) {
        if(!hazelcastInstance.<String>getSet("g7-agencies").contains(ticket.getAgencyName()))
            return;
        context.emit(new InfractionAgencyKey(hazelcastInstance.<String, Infraction>getMap("g7-violations").get(ticket.getInfractionId()).getDescription(), ticket.getAgencyName()), 1L);
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance=hazelcastInstance;
    }
}
