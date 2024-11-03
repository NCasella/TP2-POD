package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class PlateInfractionInNeighbourhood extends PlateInNeighbourhood implements DataSerializable {
    private String infractionId;

    public PlateInfractionInNeighbourhood(String plate, String neighbourhood, String infractionId) {
        super(plate, neighbourhood);
        this.infractionId = infractionId;
    }

    public PlateInfractionInNeighbourhood() {}

    public PlateInNeighbourhood getPlateInNeighbourhood() {
        return new PlateInNeighbourhood(plate, neighbourhood) ;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(plate);
        out.writeUTF(neighbourhood);
        out.writeUTF(infractionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.plate = in.readUTF();
        this.neighbourhood = in.readUTF();
        this.infractionId = in.readUTF();
    }

    @Override
    public int hashCode() {
        return Objects.hash(plate,infractionId,neighbourhood);
    }

    @Override
    public boolean equals(Object o) {
        if ( o == this) return true;
        if ( o instanceof PlateInfractionInNeighbourhood p)
            return p.plate.equals(plate) && p.infractionId.equals(infractionId) && p.neighbourhood.equals(neighbourhood);
        return false;
    }

    public String getInfractionId() {
        return infractionId;
    }

    @Override
    public String toString() {
        return plate + " " + neighbourhood + " " + infractionId;
    }
}
