package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.MappingIterator;
import com.markosindustries.distroboy.schemas.Value;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class PersistedData<I> {
  public static final ConcurrentMap<UUID, PersistedData<?>> STORED_REFERENCES =
      new ConcurrentHashMap<>();

  final Supplier<Iterator<I>> iteratorSupplier;
  final Serialiser<I> serialiser;

  public PersistedData(Supplier<Iterator<I>> iteratorSupplier, Serialiser<I> serialiser) {
    this.iteratorSupplier = iteratorSupplier;
    this.serialiser = serialiser;
  }

  public Iterator<I> getIterator() {
    return iteratorSupplier.get();
  }

  public Iterator<Value> getSerialisingIterator() {
    return new MappingIterator<>(iteratorSupplier.get(), serialiser::serialise);
  }

  public Iterator<ValueWithSerialiser<I>> getIteratorWithSerialiser() {
    return new MappingIterator<>(
        iteratorSupplier.get(), v -> new ValueWithSerialiser<>(v, serialiser));
  }

  public static class ValueWithSerialiser<T> {
    T value;
    Serialiser<T> serialiser;

    public ValueWithSerialiser(T value, Serialiser<T> serialiser) {
      this.value = value;
      this.serialiser = serialiser;
    }

    Value serialise() {
      return serialiser.serialise(value);
    }
  }
}
