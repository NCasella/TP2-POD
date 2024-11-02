package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.models.PlateInNeighbourhood;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class ReincidentPlatesPerNeighbourhoodCombinerFactory implements CombinerFactory<PlateInNeighbourhood,Integer,Integer> {
    @Override
    public Combiner<Integer,Integer> newCombiner(PlateInNeighbourhood plateInNeighbourhood) {
        return new ReincidentPlatesPerNeighbourhoodCombiner();
    }

    private static class ReincidentPlatesPerNeighbourhoodCombiner extends Combiner<Integer,Integer> {
        private int sum = 0;

        @Override
        public Integer finalizeChunk() {
            return sum;
        }

        @Override
        public void combine(Integer value) {
            sum += value;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }
}
