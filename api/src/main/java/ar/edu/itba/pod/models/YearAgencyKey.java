package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.Year;
import java.util.Objects;

// todo: q implemente COmparator o algo asi para armar el orden
public class YearAgencyKey implements DataSerializable {
    private int year;
    private String agency;

    public YearAgencyKey() {}

    public YearAgencyKey(int year, String agency) {
        this.year = year;
        this.agency = agency;
    }

    public int getYear() {
        return year;
    }

    public String getAgency() {
        return agency;
    }

    @Override
    public int hashCode() {
        return Objects.hash(agency, year);
    }

    @Override
    public boolean equals(Object o) {
        if ( o == this) return true;
        if ( o instanceof YearAgencyKey k)
            return k.agency.equals(agency) && k.year == year;
        return false;
    }
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(year);
        out.writeUTF(agency);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.year = in.readInt();
        this.agency = in.readUTF();
    }
}
