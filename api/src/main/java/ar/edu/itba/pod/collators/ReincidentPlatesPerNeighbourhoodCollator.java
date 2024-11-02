package ar.edu.itba.pod.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReincidentPlatesPerNeighbourhoodCollator implements Collator<Map.Entry<String,Double>, List<Map.Entry<String,Double>>> {

    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Double>> values) {
        return StreamSupport.stream(values.spliterator(),false) // no existen muchos varios, asi que no conviene parallel
                .filter(entry -> entry.getValue() > 0.0)
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey()))
                .collect(Collectors.toList());
    }
}
