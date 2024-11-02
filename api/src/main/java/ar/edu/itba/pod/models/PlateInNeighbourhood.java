package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class PlateInNeighbourhood implements DataSerializable {
    private String plate;
    private String neighbourhood;

    public PlateInNeighbourhood(String plate, String neighbourhood) {
        this.plate = plate;
        this.neighbourhood = neighbourhood;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(plate);
        out.writeUTF(neighbourhood);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.plate = in.readUTF();
        this.neighbourhood = in.readUTF();
    }

    @Override
    public int hashCode() {
        return Objects.hash(plate,neighbourhood) ;
    }

    @Override
    public boolean equals(Object o) {
        if ( o == this) return true;
        if ( o instanceof PlateInNeighbourhood p)
            return p.plate.equals(plate) && p.neighbourhood.equals(neighbourhood);
        return false;
    }

    public PlateInNeighbourhood() {}

    public String getPlate() {
        return plate;
    }

    public String getNeighbourhood() {
        return neighbourhood;
    }

    @Override
    public String toString() {
        return plate + " " + neighbourhood;
    }
}
