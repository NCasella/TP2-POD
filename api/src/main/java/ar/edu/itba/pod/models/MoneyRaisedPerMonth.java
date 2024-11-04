package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class MoneyRaisedPerMonth implements DataSerializable {
    private static final int MONTHS = 12;
    private final double[] months = new double[MONTHS];

    public MoneyRaisedPerMonth() {}
// todo
    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {

    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public MoneyRaisedPerMonth getYDT() {
        MoneyRaisedPerMonth result = new MoneyRaisedPerMonth();
        result.months[0] = months[0];
        for (int i = 1; i < MONTHS; i++) {
            result.months[i] += months[i] + result.months[i-1];
        }
        return result;
    }

    // cuando imprima,
    // chequeo si = al mes anterior
    public boolean monthYDT(int month) {
        if ( month > 0 )
            return months[month] == months[month - 1];
        return months[month] >0;

    }
}
