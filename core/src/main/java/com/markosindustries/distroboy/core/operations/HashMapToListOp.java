package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import java.util.Map;

/**
 * Transform each Map.Entry in the data set using the given mapping operation. Purely here for
 * the convenience of being able to provide a lambda which takes a key/value as separate
 * parameters.
 *
 * @param <K> The type of input Map keys
 * @param <V> The type of input Map values
 * @param <O> The type of values in the output list
 */
public interface HashMapToListOp<K, V, O> extends ListOp<Map.Entry<K, V>, O> {
  O map(K key, V value);

  @Override
  default IteratorWithResources<O> apply(IteratorWithResources<Map.Entry<K, V>> input)
      throws Exception {
    return new MappingIteratorWithResources<>(
        input, entry -> this.map(entry.getKey(), entry.getValue()));
  }
}
