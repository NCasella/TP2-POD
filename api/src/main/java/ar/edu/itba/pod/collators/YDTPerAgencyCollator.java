package ar.edu.itba.pod.collators;

import ar.edu.itba.pod.models.MoneyRaisedPerMonth;
import ar.edu.itba.pod.models.YearAgencyKey;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class YDTPerAgencyCollator implements Collator<Map.Entry<YearAgencyKey, MoneyRaisedPerMonth>, List<Map.Entry<YearAgencyKey,MoneyRaisedPerMonth>>> {
    private final static Comparator<YearAgencyKey> criteria = Comparator.comparing(YearAgencyKey::getAgency).thenComparing(YearAgencyKey::getYear);

    @Override
    public List<Map.Entry<YearAgencyKey, MoneyRaisedPerMonth>> collate(Iterable<Map.Entry<YearAgencyKey, MoneyRaisedPerMonth>> values) {
        return StreamSupport.stream(values.spliterator(),false)
                .filter(entry -> entry.getValue().hasRaisedMoneyYDT())
                .sorted(Map.Entry.comparingByKey(criteria))
                .collect(Collectors.toList());
    }

}