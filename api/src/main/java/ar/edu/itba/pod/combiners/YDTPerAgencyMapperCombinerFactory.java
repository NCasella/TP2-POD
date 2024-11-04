package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.models.MoneyRaisedPerMonth;
import ar.edu.itba.pod.models.MonthYearAgencyKey;
import ar.edu.itba.pod.models.Pair;
import ar.edu.itba.pod.models.YearAgencyKey;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.time.Month;

public class YDTPerAgencyMapperCombinerFactory implements CombinerFactory<YearAgencyKey, Pair<Month,Integer>, MoneyRaisedPerMonth> {

    @Override
    public Combiner<Pair<Month, Integer>, MoneyRaisedPerMonth> newCombiner(YearAgencyKey yearAgencyKey) {
        return new YDTPerAgencyMapperCombiner();
    }

    private static class YDTPerAgencyMapperCombiner extends Combiner<Pair<Month, Integer>, MoneyRaisedPerMonth> {
        private MoneyRaisedPerMonth moneyRaisedPerMonth = new MoneyRaisedPerMonth();

        @Override
        public void reset() {
            moneyRaisedPerMonth = new MoneyRaisedPerMonth();
        }

        @Override
        public void combine(Pair<Month, Integer> pair) {
            moneyRaisedPerMonth.addFineAmountToMonth(pair.getFirst().ordinal(), pair.getSecond());
        }

        @Override
        public MoneyRaisedPerMonth finalizeChunk() {
            return moneyRaisedPerMonth;
        }
    }
}