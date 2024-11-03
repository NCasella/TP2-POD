package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.models.PlateInfractionInNeighbourhood;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class ReincidentPlatesInNeighbourhoodByInfractionTypeCombinerFactory implements CombinerFactory<PlateInfractionInNeighbourhood,Integer,Integer> {
    @Override
    public Combiner<Integer,Integer> newCombiner(PlateInfractionInNeighbourhood plateInfractionInNeighbourhood) {
        return new ReincidentPlatesPerNeighbourhoodByInfractionTypeCombiner();
    }

    private static class ReincidentPlatesPerNeighbourhoodByInfractionTypeCombiner extends Combiner<Integer,Integer> {
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
