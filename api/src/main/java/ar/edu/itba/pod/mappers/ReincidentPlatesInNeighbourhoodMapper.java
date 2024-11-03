package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Cities;
import ar.edu.itba.pod.models.PlateInNeighbourhood;
import ar.edu.itba.pod.models.PlateInfractionInNeighbourhood;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDate;

public class ReincidentPlatesInNeighbourhoodMapper implements Mapper<PlateInfractionInNeighbourhood,Integer,PlateInNeighbourhood,Boolean> {
    private final int n;

    public ReincidentPlatesInNeighbourhoodMapper(int n) {
        this.n = n;
    }

    // acá se define si la patente es incidente
    @Override
    public void map(PlateInfractionInNeighbourhood plateInfractionInNeighbourhood, Integer tickets, Context<PlateInNeighbourhood, Boolean> context) {
        context.emit(plateInfractionInNeighbourhood.getPlateInNeighbourhood(), tickets>=n );
    }
}
