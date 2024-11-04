package ar.edu.itba.pod.collators;

import ar.edu.itba.pod.models.InfractionAgencyKey;
import ar.edu.itba.pod.models.InfractionFinesDifferences;
import ar.edu.itba.pod.reducers.FineAmountsPerInfractionReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FineAmountsPerInfractionCollator implements Collator<Map.Entry<String, InfractionFinesDifferences>, List<Map.Entry<String,InfractionFinesDifferences>>> {
    private int nParam;
    public FineAmountsPerInfractionCollator(int n){
        nParam = n;
    }
    @Override
    public List<Map.Entry<String, InfractionFinesDifferences>> collate(Iterable<Map.Entry<String, InfractionFinesDifferences>> values){
        SortedSet<Map.Entry<String, InfractionFinesDifferences>> result = new TreeSet<>(((o1, o2) -> o1.getValue().compareTo(o2.getValue())));
        values.forEach(result::add);
        return result.size() <= nParam? result.stream().toList() : result.stream().toList().subList(0, nParam-1);

//        return StreamSupport.stream(values.spliterator(), false)
//                .sorted(Map.Entry.comparingByValue())
//                .collect(Collectors.toList()).subList(0, nParam-1);
    }
}
