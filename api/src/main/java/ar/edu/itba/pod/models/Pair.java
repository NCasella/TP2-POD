package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class Pair<V,T> implements DataSerializable {
    private V first;
    private T second;
    public Pair(V first, T second) {
        this.first = first;
        this.second = second;
    }

    public Pair() {}

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public boolean equals(Object o) {
        if ( o == this) return true;
        if ( o instanceof Pair<?,?> p)
            return p.first.equals(first) && p.second.equals(second);
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(first);
        out.writeObject(second);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.first = in.readObject();
        this.second = in.readObject();
    }

    public V getFirst() {
        return first;
    }

    public T getSecond() {
        return second;
    }
}
