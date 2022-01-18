package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.MappingIterator;
import com.markosindustries.distroboy.schemas.Value;
import java.util.Iterator;
import java.util.function.Supplier;

public class PersistedDataReference<I> {
  final Supplier<Iterator<I>> iteratorSupplier;
  final Serialiser<I> serialiser;

  public PersistedDataReference(Supplier<Iterator<I>> iteratorSupplier, Serialiser<I> serialiser) {
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
