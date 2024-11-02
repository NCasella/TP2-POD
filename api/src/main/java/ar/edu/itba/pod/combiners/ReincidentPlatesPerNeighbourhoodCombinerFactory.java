package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.models.IncidentPlatesCount;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class ReincidentPlatesPerNeighbourhoodCombinerFactory implements CombinerFactory<String,IncidentPlatesCount,IncidentPlatesCount> {
    @Override
    public Combiner<IncidentPlatesCount,IncidentPlatesCount> newCombiner(String neighbourhood) {
        return new ReincidentPlatesPerNeighbourhoodCombiner();
    }

    private static class ReincidentPlatesPerNeighbourhoodCombiner extends Combiner<IncidentPlatesCount,IncidentPlatesCount> {
        private IncidentPlatesCount total = new IncidentPlatesCount();

        @Override
        public IncidentPlatesCount finalizeChunk() {
            return total;
        }

        @Override
        public void combine(IncidentPlatesCount i) {
            total.addIncidentPlates(i);
        }

        @Override
        public void reset() {
            total = new IncidentPlatesCount();
        }
    }
}
