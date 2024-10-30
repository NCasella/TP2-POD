package ar.edu.itba.pod;

import java.time.LocalDateTime;

public enum Cities {
    NY(4,1,3,5,0,2),
    CH(0,4,2,1,3,5);

    private final int issueDateIndex;
    private final int infractionIdIndex;
    private final int agencyIndex;
    private final int neighbourhoodIndex;
    private final int plateNumberIndex;
    private final int fineAmountIndex;

    Cities(int issueDateIndex, int infractionIdIndex, int agencyIndex, int neighbourhoodIndex, int plateNumberIndex, int fineAmountIndex){
        this.issueDateIndex=issueDateIndex;
        this.infractionIdIndex=infractionIdIndex;
        this.agencyIndex=agencyIndex;
        this.neighbourhoodIndex=neighbourhoodIndex;
        this.plateNumberIndex=plateNumberIndex;
        this.fineAmountIndex=fineAmountIndex;
    }

    public Ticket getTicket( String[] line ) {
        return new Ticket(line[plateNumberIndex] , line[infractionIdIndex], Integer.parseInt(line[fineAmountIndex]), LocalDateTime.parse(line[issueDateIndex]),  line[agencyIndex], line[neighbourhoodIndex] );
    }
}
