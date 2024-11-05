package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.*;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.Month;
import java.time.YearMonth;
import java.util.Set;


public class YDTPerAgencyMapper implements Mapper<Integer,String,YearAgencyKey, Pair<Month,Integer>> {

    private final Cities city;
    private final Set<String> agencies;

    public YDTPerAgencyMapper(Cities city, Set<String> agencies) {
        this.city = city;
        this.agencies = agencies;
    }

    @Override
    public void map(Integer integer, String line, Context<YearAgencyKey, Pair<Month, Integer>> context) {
        final String[] s = line.split(";");
        final String agency = city.getAgency(s);

        if ( agencies.contains(agency) ) {
            final YearMonth yearMonth = city.getYearMonthFromIssueDate(s);
            context.emit( new YearAgencyKey(yearMonth.getYear(),agency), new Pair<>(yearMonth.getMonth(), city.getFineAmount(s)));
        }
    }
}