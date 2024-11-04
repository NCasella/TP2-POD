package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class InfractionDefWithAgency implements DataSerializable {
    private String infractionId;
    private String agencyName;

    public String getInfractionId() {
        return infractionId;
    }

    public String getAgencyName(){return this.agencyName;}

    public InfractionDefWithAgency(){}

    public InfractionDefWithAgency(String infractionId, String agency){
        this.infractionId = infractionId;
        this.agencyName = agency;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(infractionId);
        out.writeUTF(agencyName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.infractionId = in.readUTF();
        this.agencyName = in.readUTF();
    }
}
