package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.PlateInNeighbourhood;
import ar.edu.itba.pod.models.IncidentPlatesCount;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class ReincidentPlatesPerNeighbourhoodMapper implements Mapper<PlateInNeighbourhood,Boolean,String, IncidentPlatesCount> {

    @Override
    public void map(PlateInNeighbourhood plateInNeighbourhood, Boolean isReincident, Context<String, IncidentPlatesCount> context) {
        context.emit(plateInNeighbourhood.getNeighbourhood(), new IncidentPlatesCount(isReincident? 1:0, 1) );
    }
}
