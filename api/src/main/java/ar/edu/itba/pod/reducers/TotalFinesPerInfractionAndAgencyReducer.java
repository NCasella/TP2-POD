package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.models.InfractionAgencyKey;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class TotalFinesPerInfractionAndAgencyReducer implements ReducerFactory<InfractionAgencyKey,Long,Long> {
    @Override
    public Reducer<Long, Long> newReducer(InfractionAgencyKey s) {
        return new Reducer<>() {
            private long sum=0;
            @Override
            public void reduce(Long aLong) {
                sum+=aLong;
            }

            @Override
            public Long finalizeReduce() {
                return sum;
            }
        };
    }

}
