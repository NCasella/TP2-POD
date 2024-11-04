package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class MoneyRaisedPerMonth implements DataSerializable {
    private static final int MONTHS = 12;
    private long[] months = new long[MONTHS];
    private final static int LAST_MONTH = MONTHS - 1;

    public MoneyRaisedPerMonth() {}

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLongArray(months);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.months = in.readLongArray();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(months);
    }

    @Override
    public boolean equals(Object o) {
        if ( o == this) return true;
        if ( o instanceof MoneyRaisedPerMonth m)
            return Arrays.equals(months, m.months);
        return false;
    }

    public void append(MoneyRaisedPerMonth moneyRaisedPerMonth) {
        for (int i = 0; i < MONTHS; i++) {
            months[i] += moneyRaisedPerMonth.months[i];
        }
    }

    public long[] getMoneyRaisedPerMonth() {
        return months;
    }

    public void addFineAmountToMonth(int monthIndex, int amount) {
        months[monthIndex] += amount;
    }

    public MoneyRaisedPerMonth getYDT() {
        MoneyRaisedPerMonth result = new MoneyRaisedPerMonth();
        result.months[0] = months[0];
        for (int i = 1; i < MONTHS; i++) {
            result.months[i] += months[i] + result.months[i-1];
        }
        return result;
    }

    public boolean monthRaisedMoneyYDT(int monthIndex) {
        if ( monthIndex > 0 )
            return months[monthIndex] > months[monthIndex - 1];
        return months[monthIndex] >0;

    }

    public boolean hasRaisedMoneyYDT() {
        return months[LAST_MONTH] > 0;
    }

}
