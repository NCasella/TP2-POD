package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.PlateInNeighbourhood;
import ar.edu.itba.pod.models.IncidentPlatesCount;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class ReincidentPlatesPerNeighbourhoodMapper implements Mapper<PlateInNeighbourhood,Integer,String, IncidentPlatesCount> {
    private final int n;

    public ReincidentPlatesPerNeighbourhoodMapper(int n) {
        this.n = n;
    }

    @Override
    public void map(PlateInNeighbourhood plateInNeighbourhood, Integer ticketsCount, Context<String, IncidentPlatesCount> context) {
        context.emit(plateInNeighbourhood.getNeighbourhood(), new IncidentPlatesCount(n<=ticketsCount? 1:0, 1) );
    }
}
