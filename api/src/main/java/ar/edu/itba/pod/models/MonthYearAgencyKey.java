package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.Year;
import java.time.YearMonth;

public class MonthYearAgencyKey implements DataSerializable {
    private YearMonth yearMonth;
    private String agency;

    public MonthYearAgencyKey() {}

    public MonthYearAgencyKey(YearMonth yearMonth, String agency) {
        this.yearMonth = yearMonth;
        this.agency = agency;
    }

    public int getMonthNum() {
        return yearMonth.getMonthValue();
    }

    public Year getYear() {
        return Year.of(yearMonth.getYear());
    }

    public YearMonth getYearMonth() {
        return yearMonth;
    }

    public String getAgency() {
        return agency;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {

    }
}
