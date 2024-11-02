package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.models.Infraction;
import ar.edu.itba.pod.models.InfractionAgencyKey;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class TotalFinesPerInfractionAndAgencyCombiner implements CombinerFactory<InfractionAgencyKey,Long,Long> {
    @Override
    public Combiner<Long, Long> newCombiner(InfractionAgencyKey infractionAgencyKey) {
        return new Combiner<>() {
            private long sum = 0;

            @Override
            public void combine(Long count) {
                sum += count;
            }

            @Override
            public Long finalizeChunk() {
                return sum;
            }
        };
    }
}
