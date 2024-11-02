package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Cities;
import ar.edu.itba.pod.models.PlateInNeighbourhood;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ReincidentPlatesPerNeighbourhoodMapper implements Mapper<PlateInNeighbourhood,Integer,String,Integer> {
    private final int n;
    private final ConcurrentMap<String,Integer> totalTicketsPerNeighbourhood;
    public ReincidentPlatesPerNeighbourhoodMapper(int n, ConcurrentMap<String,Integer> totalTicketsPerNeighbourhood) {
        this.n = n;
        this.totalTicketsPerNeighbourhood = totalTicketsPerNeighbourhood;
    }

    @Override
    public void map(PlateInNeighbourhood plateInNeighbourhood, Integer ticketsCount, Context<String, Integer> context) {
        if ( ticketsCount >= n )
            context.emit(plateInNeighbourhood.getNeighbourhood(), 1);
        totalTicketsPerNeighbourhood.compute(plateInNeighbourhood.getNeighbourhood(), (k,v) -> v=1 + (v==null? 0 : v));
    }
}
