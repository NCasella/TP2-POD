package ar.edu.itba.pod.Collators;

import ar.edu.itba.pod.models.InfractionAgencyKey;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class TotalFinesPerInfractionAndAgencyCollator implements Collator<Map.Entry<InfractionAgencyKey, Long>, SortedSet<Map.Entry<InfractionAgencyKey, Long>>> {
    @Override
    public SortedSet<Map.Entry<InfractionAgencyKey, Long>> collate(Iterable<Map.Entry<InfractionAgencyKey, Long>> reducedResults) {
        SortedSet<Map.Entry<InfractionAgencyKey, Long>> result = new TreeSet<>((o1, o2) -> {
            int compare=Long.compare(o2.getValue(), o1.getValue());
            if(compare==0){
                compare=o1.getKey().getInfraction().compareTo(o2.getKey().getInfraction());
            }
            if(compare==0){
                compare=o1.getKey().getAgency().compareTo(o2.getKey().getInfraction());
            }
        return compare;
        });
        reducedResults.forEach(result::add);

        return result;
    }
}
