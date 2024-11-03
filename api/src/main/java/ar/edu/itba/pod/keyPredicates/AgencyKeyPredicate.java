package ar.edu.itba.pod.keyPredicates;


import com.hazelcast.mapreduce.KeyPredicate;

public class AgencyKeyPredicate implements KeyPredicate<String> {
    String agencyFilter;
    public AgencyKeyPredicate(String agencyFilter){

        this.agencyFilter = agencyFilter;
    }
    @Override
    public boolean evaluate(String key){
        return key.equals(agencyFilter);
    }
}
