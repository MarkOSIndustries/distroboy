package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIterator;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import com.markosindustries.distroboy.schemas.Value;
import java.util.Iterator;

/**
 * A specification for turning objects into a common format ({@link Value}) for sending between
 * cluster nodes, or storage in memory/disk.
 *
 * @param <T> The type that this {@link Serialiser} can serialise/deserialise
 */
public interface Serialiser<T> {
  /**
   * Convert the given object to a {@link Value}
   *
   * @param value The object to serialise
   * @return A {@link Value} representation of the given object
   * @throws Exception if serialisation fails
   */
  Value serialise(T value) throws Exception;

  /**
   * Convert the given object to a {@link Value}, rethrowing exceptions as {@link RuntimeException}s
   *
   * @param value The object to serialise
   * @return A {@link Value} representation of the given object
   * @throws RuntimeException if serialisation fails
   */
  default Value serialiseUnchecked(T value) {
    try {
      return serialise(value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert the given {@link Value} back to the expected object type
   *
   * @param value The {@link Value} to deserialise
   * @return The deserialised object
   * @throws Exception if deserialisation fails
   */
  T deserialise(Value value) throws Exception;

  /**
   * Convert the given {@link Value} back to the expected object type, rethrowing exceptions as
   * {@link RuntimeException}s
   *
   * @param value The {@link Value} to deserialise
   * @return The deserialised object
   * @throws RuntimeException if deserialisation fails
   */
  default T deserialiseUnchecked(Value value) {
    try {
      return deserialise(value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convenience method for converting an iterator of {@link Value}s to an iterator of objects by
   * deserialising each item
   *
   * @param values The {@link Iterator} of {@link Value}s to deserialise
   * @return An {@link Iterator} of deserialised objects
   */
  default Iterator<T> deserialiseIterator(Iterator<Value> values) {
    return new MappingIterator<>(values, this::deserialiseUnchecked);
  }

  /**
   * Convenience method for converting an {@link IteratorWithResources} of {@link Value}s to an
   * iterator of objects by deserialising each item
   *
   * @param values The {@link Iterator} of {@link Value}s to deserialise
   * @return An {@link Iterator} of deserialised objects
   */
  default IteratorWithResources<T> deserialiseIteratorWithResources(
      IteratorWithResources<Value> values) {
    return new MappingIteratorWithResources<>(values, this::deserialiseUnchecked);
  }
}
