package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.models.MoneyRaisedPerMonth;
import ar.edu.itba.pod.models.MonthYearAgencyKey;
import ar.edu.itba.pod.models.Pair;
import ar.edu.itba.pod.models.YearAgencyKey;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.time.Month;
import java.util.Arrays;

public class YDTPerAgencyReducerFactory implements ReducerFactory<YearAgencyKey,MoneyRaisedPerMonth,MoneyRaisedPerMonth> {

    @Override
    public Reducer<MoneyRaisedPerMonth, MoneyRaisedPerMonth> newReducer(YearAgencyKey yearAgencyKey) {
        return new YDTPerAgencyReducer();
    }

    private static class YDTPerAgencyReducer extends Reducer<MoneyRaisedPerMonth,MoneyRaisedPerMonth> {
        private MoneyRaisedPerMonth totalMoneyRaisedPerMonth = new MoneyRaisedPerMonth();

        @Override
        public void reduce(MoneyRaisedPerMonth moneyRaisedPerMonth) {
            totalMoneyRaisedPerMonth.append(moneyRaisedPerMonth);
        }

        @Override
        public MoneyRaisedPerMonth finalizeReduce() {
            return totalMoneyRaisedPerMonth.getYDT();
        }
    }
}
