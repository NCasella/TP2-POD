package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.models.IncidentPlatesCount;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class ReincidentPlatesPerNeighbourhoodReducerFactory implements ReducerFactory<String, IncidentPlatesCount,Double> {


    @Override
    public Reducer<IncidentPlatesCount, Double> newReducer(String neighbourhood) {
        return new ReincidentPlatesPerNeighbourhoodReducer();
    }

    private static class ReincidentPlatesPerNeighbourhoodReducer extends Reducer<IncidentPlatesCount,Double> {
        private IncidentPlatesCount total = new IncidentPlatesCount();

        @Override
        public void reduce(IncidentPlatesCount i) {
            total.addIncidentPlates(i);
        }

        @Override
        public Double finalizeReduce() {
            return total.getPercentage();
        }
    }
}
