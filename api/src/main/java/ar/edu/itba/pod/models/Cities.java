package ar.edu.itba.pod.models;

import java.time.LocalDate;

import java.time.format.DateTimeFormatter;

public enum Cities {
    NYC(4,1,3,5,0,2,DateTimeFormatter.ofPattern("yyyy-MM-dd")),
    CHI(0,4,2,1,3,5,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

    private final int issueDateIndex;
    private final int infractionIdIndex;
    private final int agencyIndex;
    private final int neighbourhoodIndex;
    private final int plateNumberIndex;
    private final int fineAmountIndex;
    private final DateTimeFormatter dateFormatter;
    public int getIssueDateIndex() {
        return issueDateIndex;
    }

    public int getInfractionIdIndex() {
        return infractionIdIndex;
    }

    public int getAgencyIndex() {
        return agencyIndex;
    }

    public int getNeighbourhoodIndex() {
        return neighbourhoodIndex;
    }

    public int getPlateNumberIndex() {
        return plateNumberIndex;
    }

    public int getFineAmountIndex() {
        return fineAmountIndex;
    }

    Cities(int issueDateIndex, int infractionIdIndex, int agencyIndex, int neighbourhoodIndex, int plateNumberIndex, int fineAmountIndex,DateTimeFormatter dateFormatter){
        this.issueDateIndex=issueDateIndex;
        this.infractionIdIndex=infractionIdIndex;
        this.agencyIndex=agencyIndex;
        this.neighbourhoodIndex=neighbourhoodIndex;
        this.plateNumberIndex=plateNumberIndex;
        this.fineAmountIndex=fineAmountIndex;
        this.dateFormatter=dateFormatter;
    }

    public Ticket getTicket(String[] line ) {
        return new Ticket(line[plateNumberIndex] , line[infractionIdIndex], line[fineAmountIndex].indexOf('.')>= 0 ? Integer.parseInt(line[fineAmountIndex].substring(0,line[fineAmountIndex].indexOf('.'))) : Integer.parseInt(line[fineAmountIndex]), LocalDate.parse(line[issueDateIndex],this.dateFormatter),  line[agencyIndex], line[neighbourhoodIndex] );
    }

    public LocalDate getIssueDate( String[] line ) {
        return LocalDate.parse(line[issueDateIndex], dateFormatter);
    }

    public PlateInfractionInNeighbourhood getPlateInfractionInNeighbourhood(String[] line) {
        return new PlateInfractionInNeighbourhood(line[plateNumberIndex],line[neighbourhoodIndex], line[infractionIdIndex] );

    }
}
