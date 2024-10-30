package ar.edu.itba.pod;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Infraction implements DataSerializable {
    private String infractionId;
    private String description;

    public Infraction(String infractionId, String description){
        this.infractionId = infractionId;
        this.description = description;
    }

    public Infraction() {};

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(infractionId);
        out.writeUTF(description);
    }

    public String getInfractionId(){return this.infractionId;}

    public String getDescription() {
        return description;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.infractionId = in.readUTF();
        this.description = in.readUTF();
    }

}
//○ Code: Código Identificador (Entero)
//○ Definition: Infracción (Cadena de caracteres)

//○ violation_code: Código Identificador (Cadena de caracteres)
//○ violation_description: Infracción (Cadena de caracteres)
