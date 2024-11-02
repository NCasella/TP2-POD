package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Cities;
import ar.edu.itba.pod.models.PlateInNeighbourhood;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDate;

public class ReincidentPlatesPerNeighbourhoodMapper implements Mapper<Integer, String, PlateInNeighbourhood, Integer> {
    private final LocalDate fromDate;
    private final LocalDate toDate;
    private final Cities city;

    public ReincidentPlatesPerNeighbourhoodMapper(LocalDate from, LocalDate to, Cities city) {
        this.fromDate = from;
        this.toDate = to;
        this.city = city;
    }

    @Override
    public void map(Integer line, String document, Context<PlateInNeighbourhood, Integer> context) {
        final String[] s = document.split(";");
        if ( city.getIssueDate(s).isBefore(fromDate) || city.getIssueDate(s).isAfter(toDate) )
            return;
        context.emit(city.getPlateInNeighbourhood(s), 1);
    }
}
