package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class InfractionFinesDifferences implements DataSerializable, Comparable<InfractionFinesDifferences>{
   private String infraction;
   private int maxFine;
   private int minFine;
   private int diff;

   public InfractionFinesDifferences(){}
   public InfractionFinesDifferences(String infractionId, int minFine, int maxFine){
       this.infraction = infractionId;
       this.maxFine = maxFine;
       this.minFine = minFine;
       this.diff = maxFine - minFine;
   }

    public int getMinFine() {
        return minFine;
    }

    public int getMaxFine() {
        return maxFine;
    }

    public String getInfraction() {
        return infraction;
    }

    public int getDiff() {
        return diff;
    }

    public void setInfraction(String infraction) {
        this.infraction = infraction;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || (obj instanceof InfractionFinesDifferences inf && this.infraction.equals(inf.infraction) && this.minFine == inf.minFine && this.maxFine== inf.maxFine);
    }
    @Override
    public int hashCode(){
       return Objects.hash(infraction);
    }

    @Override
    public String toString(){
       return infraction + ";" + minFine + ";" + maxFine + ";" + diff;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
       out.writeUTF(infraction);
       out.writeInt(minFine);
       out.writeInt(maxFine);
       out.writeInt(diff);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
       this.infraction = in.readUTF();
       this.minFine = in.readInt();
       this.maxFine = in.readInt();
       this.diff = in.readInt();
    }


    @Override
    public int compareTo(InfractionFinesDifferences o2){
       if (diff == o2.diff){
           return infraction.compareTo(o2.infraction);
       }
       return o2.diff - diff;
    }

}
