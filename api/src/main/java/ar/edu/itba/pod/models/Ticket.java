package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDate;

public class Ticket implements DataSerializable {

    private String plateNumber;
    private String infractionId;
    private int fineAmount;
    private LocalDate issueDate;
    private String agencyName;
    private String neighbourhood;

    public String getPlateNumber() {
        return plateNumber;
    }

    public String getInfractionId() {
        return infractionId;
    }

    public LocalDate getIssueDate() {
        return issueDate;
    }

    public int getFineAmount() {
        return fineAmount;
    }

    public String getAgencyName() {
        return agencyName;
    }

    public String getNeighbourhood() {
        return neighbourhood;
    }

    public void setInfractionId(String infractionId) {
        this.infractionId = infractionId;
    }

    public Ticket(){}

    public Ticket(String plateNumber, String infractionId, int fineAmount, LocalDate issueDate, String agency, String neighbourhood ){
        this.plateNumber = plateNumber;
        this.infractionId = infractionId;
        this.fineAmount = fineAmount;
        this.issueDate = issueDate;
        this.agencyName = agency;
        this.neighbourhood=neighbourhood;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(plateNumber);
        out.writeUTF(infractionId);
        out.writeInt(fineAmount);
        out.writeObject(issueDate);
        out.writeUTF(agencyName);
        out.writeUTF(neighbourhood);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.plateNumber = in.readUTF();
        this.infractionId = in.readUTF();
        this.fineAmount = in.readInt();
        this.issueDate = in.readObject();
        this.agencyName = in.readUTF();
        this.neighbourhood = in.readUTF();
    }
}
//chicago
//○ Plate: Patente (Cadena de caracteres)
//○ Infraction ID: Identificador de la infracción (Entero)
//○ Fine Amount: Monto (Número)
//agency      ○ Issuing Agency: Agencia (Cadena de caracteres)
//date     ○ Issue Date: Fecha de la multa (Formato YYYY-MM-DD)
//barrio ○ County Name: Barrio (Cadena de caracteres)

// issue_date: Fecha y hora de la multa (Formato YYYY-MM-DD hh:mm:ss)
//○ community_area_name: Barrio (Cadena de caracteres)
//○ unit_description: Agencia (Cadena de caracteres)
//○ license_plate_number: Patente (UUID)
//○ violation_code: Código de la infracción (Cadena de caracteres)
//○ fine_amount: Monto (Entero)