package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import com.markosindustries.distroboy.schemas.Value;
import java.util.function.Supplier;

/**
 * Represents a piece of data on a cluster member which can be distributed
 *
 * @param <I> The type of the distributable values
 */
public class DistributableDataReference<I> {
  final Supplier<IteratorWithResources<I>> iteratorSupplier;
  final Serialiser<I> serialiser;
  private final boolean reiterable;

  /**
   * Represents a piece of data on a cluster member which can be distributed.
   *
   * @see com.markosindustries.distroboy.core.Cluster#addDistributableData(DataReferenceId,
   *     DistributableDataReference)
   * @param iteratorSupplier Provide an iterator for the values when being retrieved
   * @param serialiser The serialiser for the values the iterator will supply
   * @param reiterable <code>true</code> if the data reference can be retrieved and iterated more
   *     than once, otherwise <code>false</code>
   */
  public DistributableDataReference(
      Supplier<IteratorWithResources<I>> iteratorSupplier,
      Serialiser<I> serialiser,
      boolean reiterable) {
    this.iteratorSupplier = iteratorSupplier;
    this.serialiser = serialiser;
    this.reiterable = reiterable;
  }

  /**
   * Convenience method to iterate and serialise the values at this reference. Necessary due to type
   * erasure when retrieving the referenced data - the compiler can't prove that the serialiser type
   * matches the iterator type
   *
   * @return An iterator of serialised {@link Value}s
   */
  IteratorWithResources<Value> getSerialisingIterator() {
    return new MappingIteratorWithResources<>(iteratorSupplier.get(), serialiser::serialise);
  }

  /**
   * Convenience method to iterate and values at this reference with the ability to serialise each
   * entry. Necessary due to type erasure when retrieving the referenced data - the compiler can't
   * prove that the serialiser type matches the iterator type
   *
   * @return An iterator of {@link ValueWithSerialiser}s
   */
  IteratorWithResources<ValueWithSerialiser<I>> getIteratorWithSerialiser() {
    return new MappingIteratorWithResources<>(
        iteratorSupplier.get(), v -> new ValueWithSerialiser<>(v, serialiser));
  }

  public boolean isReiterable() {
    return this.reiterable;
  }

  static class ValueWithSerialiser<T> {
    final T value;
    final Serialiser<T> serialiser;

    public ValueWithSerialiser(T value, Serialiser<T> serialiser) {
      this.value = value;
      this.serialiser = serialiser;
    }

    Value serialise() {
      return serialiser.serialise(value);
    }
  }
}
