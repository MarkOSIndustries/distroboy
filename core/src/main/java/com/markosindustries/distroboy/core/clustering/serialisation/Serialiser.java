package com.markosindustries.distroboy.core.clustering.serialisation;

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
   */
  Value serialise(T value);

  /**
   * Convert the given {@link Value} back to the expected object type
   *
   * @param value The {@link Value} to deserialise
   * @return The deserialised object
   * @throws Exception if deserialisation fails
   */
  T deserialise(Value value) throws Exception;

  /**
   * Convenience method for converting an iterator of {@link Value}s to an iterator of objects by
   * deserialising each item
   *
   * @param values The {@link Iterator} of {@link Value}s to deserialise
   * @return An {@link Iterator} of deserialised objects
   */
  default Iterator<T> deserialiseIterator(Iterator<Value> values) {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return values.hasNext();
      }

      @Override
      public T next() {
        try {
          return deserialise(values.next());
        } catch (Exception e) {
          // TODO: is this the right thing to do here?
          throw new RuntimeException(e);
        }
      }
    };
  }
}
