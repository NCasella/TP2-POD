package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public class InfractionAgencyKey implements DataSerializable {
    private String infraction;
    private String agency;

    public InfractionAgencyKey(String infraction, String agency) {
        this.infraction = infraction;
        this.agency = agency;
    }

    InfractionAgencyKey(){}

    public String getInfraction() {
        return infraction;
    }

    public String getAgency() {
        return agency;
    }

    @Override
    public boolean equals(Object o) {
        if(this==o)
            return true;
        if(!(o instanceof InfractionAgencyKey in))
            return false;
        return this.agency.equals(in.agency) && this.infraction.equals(in.infraction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(infraction, agency);
    }

    @Override
    public String toString() {
        return infraction + ";" + agency;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(infraction);
        objectDataOutput.writeUTF(agency);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.infraction=objectDataInput.readUTF();
        this.agency=objectDataInput.readUTF();
    }

}