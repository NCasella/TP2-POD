package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Cities;
import ar.edu.itba.pod.models.MonthYearAgencyKey;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class MoneyRaisedPerYearMonthAgencyMapper implements Mapper<Integer,String, MonthYearAgencyKey,Integer> {
    private final Cities city;

    public MoneyRaisedPerYearMonthAgencyMapper(Cities city) {
        this.city = city;
    }

    @Override
    public void map(Integer integer, String line, Context<MonthYearAgencyKey, Integer> context) {
        final String[] s = line.split(";");
        // todo: filtrar agencias q no estan en csv
        context.emit(city.getMonthYearAgencyKey(s), 1);
    }
}
