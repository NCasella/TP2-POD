package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class IncidentPlatesCount implements DataSerializable {
    private int gten;
    private int total;

    public IncidentPlatesCount() {}

    public IncidentPlatesCount(int gten, int total) {
        this.gten = gten;
        this.total = total;
    }

    public int getGten() {
        return gten;
    }

    public int getTotal() {
        return total;
    }

    public void addIncidentPlates (IncidentPlatesCount i) {
        gten += i.gten;
        total += i.total;
    }

    public double getPercentage() {
        return Math.floor( ((gten * 100.0) / total) * 100) / 100;
    }

    @Override
    public int hashCode() {
        return Objects.hash(gten, total);
    }

    @Override
    public boolean equals(Object o) {
        if ( this == o ) return true;
        if ( o instanceof IncidentPlatesCount i )
            return Objects.equals(gten, i.gten) && Objects.equals(total, i.total);
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(gten);
        out.writeInt(total);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.gten = in.readInt();
        this.total = in.readInt();
    }
}
