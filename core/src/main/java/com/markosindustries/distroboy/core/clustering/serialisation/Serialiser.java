package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.Value;
import java.util.Iterator;

public interface Serialiser<T> {
  Value serialise(T value);

  T deserialise(Value value) throws Exception;

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
