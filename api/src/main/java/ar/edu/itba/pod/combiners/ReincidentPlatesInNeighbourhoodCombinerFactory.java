package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.models.PlateInNeighbourhood;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class ReincidentPlatesInNeighbourhoodCombinerFactory implements CombinerFactory<PlateInNeighbourhood,Boolean,Boolean> {
    @Override
    public Combiner<Boolean, Boolean> newCombiner(PlateInNeighbourhood plateInNeighbourhood) {
        return new ReincidentPlatesInNeighbourhoodCombiner();
    }

    private static class ReincidentPlatesInNeighbourhoodCombiner extends Combiner<Boolean,Boolean> {
        private Boolean nOrMoreTickets = false;

        @Override
        public void combine(Boolean hasnOrMoreTickets) {
            if ( hasnOrMoreTickets )
                nOrMoreTickets = true;
        }

        @Override
        public Boolean finalizeChunk() {
            return nOrMoreTickets;
        }

        @Override
        public void reset() {
            nOrMoreTickets = false;
        }
    }
}
