package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.models.MonthYearAgencyKey;
import ar.edu.itba.pod.models.PlateInfractionInNeighbourhood;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class MoneyRaisedPerYearMonthAgencyCombinerFactory implements CombinerFactory<MonthYearAgencyKey,Integer,Integer> {
    @Override
    public Combiner<Integer, Integer> newCombiner(MonthYearAgencyKey monthYearAgencyKey) {
        return new MoneyRaisedPerYearMonthAgencyCombiner();
    }

    private static class MoneyRaisedPerYearMonthAgencyCombiner extends Combiner<Integer,Integer> {
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