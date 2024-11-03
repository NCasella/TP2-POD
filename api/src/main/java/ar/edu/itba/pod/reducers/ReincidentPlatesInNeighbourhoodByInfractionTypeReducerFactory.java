package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.models.PlateInfractionInNeighbourhood;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class ReincidentPlatesInNeighbourhoodByInfractionTypeReducerFactory implements ReducerFactory<PlateInfractionInNeighbourhood,Integer,Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(PlateInfractionInNeighbourhood plateInfractionInNeighbourhood) {
        return new ReincidentPlatesInNeighbourhoodByInfractionTypeReducer();
    }

    private static class ReincidentPlatesInNeighbourhoodByInfractionTypeReducer extends Reducer<Integer,Integer> {
        private int sum = 0;

        @Override
        public void reduce(Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }
}
