package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import java.util.Map;

/**
 * Transform each Map.Entry in the data set using the given mapping operation. Purely here for the
 * convenience of being able to provide a lambda which takes a key/value as separate parameters.
 *
 * @param <Key> The type of input Map keys
 * @param <Value> The type of input Map values
 * @param <Output> The type of values in the output list
 */
public interface HashMapToListOp<Key, Value, Output> extends ListOp<Map.Entry<Key, Value>, Output> {
  /**
   * Transform a key-value pair into the output type
   *
   * @param key The key
   * @param value The value
   * @return The result of mapping the key-value pair
   */
  Output map(Key key, Value value);

  @Override
  default IteratorWithResources<Output> apply(IteratorWithResources<Map.Entry<Key, Value>> input)
      throws Exception {
    return new MappingIteratorWithResources<>(
        input, entry -> this.map(entry.getKey(), entry.getValue()));
  }
}
