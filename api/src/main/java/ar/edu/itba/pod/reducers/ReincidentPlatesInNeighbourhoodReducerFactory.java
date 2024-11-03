package ar.edu.itba.pod.reducers;


import ar.edu.itba.pod.models.PlateInNeighbourhood;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class ReincidentPlatesInNeighbourhoodReducerFactory implements ReducerFactory<PlateInNeighbourhood,Boolean,Boolean> {
    @Override
    public Reducer<Boolean,Boolean> newReducer(PlateInNeighbourhood plateInNeighbourhood) {
        return new ReincidentPlatesPerNeighbourhoodReducer();
    }

    private static class ReincidentPlatesPerNeighbourhoodReducer extends Reducer<Boolean,Boolean> {
        private Boolean nOrMoreTickets = false;

        @Override
        public void reduce(Boolean hasnOrMoreTickets) {
            if ( hasnOrMoreTickets )
                nOrMoreTickets = true;
        }

        @Override
        public Boolean finalizeReduce() {
            return nOrMoreTickets;
        }

    }
}
