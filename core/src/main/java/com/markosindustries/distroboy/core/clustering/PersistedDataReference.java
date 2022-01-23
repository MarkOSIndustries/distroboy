package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import com.markosindustries.distroboy.schemas.Value;
import java.util.function.Supplier;

public class PersistedDataReference<I> {
  final Supplier<IteratorWithResources<I>> iteratorSupplier;
  final Serialiser<I> serialiser;

  public PersistedDataReference(
      Supplier<IteratorWithResources<I>> iteratorSupplier, Serialiser<I> serialiser) {
    this.iteratorSupplier = iteratorSupplier;
    this.serialiser = serialiser;
  }

  public IteratorWithResources<I> getIterator() {
    return iteratorSupplier.get();
  }

  public IteratorWithResources<Value> getSerialisingIterator() {
    return new MappingIteratorWithResources<>(iteratorSupplier.get(), serialiser::serialise);
  }

  public IteratorWithResources<ValueWithSerialiser<I>> getIteratorWithSerialiser() {
    return new MappingIteratorWithResources<>(
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
