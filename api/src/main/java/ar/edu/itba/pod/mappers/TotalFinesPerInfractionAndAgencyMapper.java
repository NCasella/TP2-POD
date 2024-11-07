package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.InfractionAgencyKey;
import ar.edu.itba.pod.models.InfractionDefWithAgency;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TotalFinesPerInfractionAndAgencyMapper implements Mapper<Long, InfractionDefWithAgency, InfractionAgencyKey,Long>,HazelcastInstanceAware {

    private transient HazelcastInstance hazelcastInstance;
    private final String agenciesIMap;
    private final String violationsIMap;

    public TotalFinesPerInfractionAndAgencyMapper(String agenciesIMap, String violationsIMap) {
        this.agenciesIMap = agenciesIMap;
        this.violationsIMap = violationsIMap;
    }

    @Override
    public void map(Long s, InfractionDefWithAgency ticket, Context<InfractionAgencyKey, Long> context) {
        if(!hazelcastInstance.<String>getSet(agenciesIMap).contains(ticket.getAgencyName()))
            return;
        context.emit(new InfractionAgencyKey(hazelcastInstance.<String, String>getMap(violationsIMap).get(ticket.getInfractionId()), ticket.getAgencyName()), 1L);
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance=hazelcastInstance;
    }
}
