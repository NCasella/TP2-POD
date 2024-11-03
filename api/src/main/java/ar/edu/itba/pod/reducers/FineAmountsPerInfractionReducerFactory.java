package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.models.InfractionFinesDifferences;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class FineAmountsPerInfractionReducerFactory implements ReducerFactory<String, Integer, InfractionFinesDifferences> {
    public Reducer<Integer, InfractionFinesDifferences> newReducer(String infractionId) {
        return new FineAmountsPerInfractionReducer(infractionId);
    }

    private static class FineAmountsPerInfractionReducer extends Reducer<Integer, InfractionFinesDifferences>{

        private int max = 0;
        private int min = Integer.MAX_VALUE;
        private String infractionId;
        public FineAmountsPerInfractionReducer(String infractionId){
            this.infractionId = infractionId;
        }

        @Override
        public void reduce(Integer value){
            if (value < min){
                min = value;
            }
            if (value > max){
                max = value;
            }
        }

        @Override
        public InfractionFinesDifferences finalizeReduce(){
            return new InfractionFinesDifferences(infractionId, min, max);
        }
    }
}
