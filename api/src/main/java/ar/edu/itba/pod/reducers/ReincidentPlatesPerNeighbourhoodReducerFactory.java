package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.models.PlateInNeighbourhood;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class ReincidentPlatesPerNeighbourhoodReducerFactory implements ReducerFactory<PlateInNeighbourhood,Integer,Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(PlateInNeighbourhood plateInNeighbourhood) {
        return new ReincidentPlatesPerNeighbourhoodReducer();
    }

    private static class ReincidentPlatesPerNeighbourhoodReducer extends Reducer<Integer,Integer> {
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
