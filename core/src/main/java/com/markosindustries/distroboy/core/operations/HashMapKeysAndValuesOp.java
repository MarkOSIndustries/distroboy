package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Transform the keys and values for each item in the data set independently of each other. If you
 * need to map the keys and values in context of each other - use {@link HashMapToListOp}
 *
 * @param <K> The type of input Map keys
 * @param <V> The type of input Map values
 * @param <K2> The type of output Map keys
 * @param <V2> The type of output Map values
 */
public interface HashMapKeysAndValuesOp<K, V, K2, V2>
    extends Operation<Map.Entry<K, V>, Map.Entry<K2, V2>, Map<K2, V2>> {
  /**
   * Transform a key to the output key type
   *
   * @param key The key to transform
   * @return The resulting key
   */
  K2 mapKey(K key);

  /**
   * Transform a value to the output value type
   *
   * @param value The value to transform
   * @return The resulting value
   */
  V2 mapValue(V value);

  @Override
  default IteratorWithResources<Map.Entry<K2, V2>> apply(
      IteratorWithResources<Map.Entry<K, V>> input) throws Exception {
    return new IteratorWithResources<Map.Entry<K2, V2>>() {
      @Override
      public boolean hasNext() {
        return input.hasNext();
      }

      @Override
      public Map.Entry<K2, V2> next() {
        final var next = input.next();
        return new AbstractMap.SimpleImmutableEntry<>(
            mapKey(next.getKey()), mapValue(next.getValue()));
      }

      @Override
      public void close() throws Exception {
        input.close();
      }
    };
  }

  @Override
  default Map<K2, V2> collect(Iterator<Map.Entry<K2, V2>> results) {
    return IteratorTo.map(results);
  }
}
