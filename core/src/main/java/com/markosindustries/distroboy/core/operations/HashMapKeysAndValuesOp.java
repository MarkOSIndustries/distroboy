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
 * @param <InputKey> The type of input Map keys
 * @param <InputValue> The type of input Map values
 * @param <OutputKey> The type of output Map keys
 * @param <OutputValue> The type of output Map values
 */
public interface HashMapKeysAndValuesOp<InputKey, InputValue, OutputKey, OutputValue>
    extends Operation<
        Map.Entry<InputKey, InputValue>,
        Map.Entry<OutputKey, OutputValue>,
        Map<OutputKey, OutputValue>> {
  /**
   * Transform a key to the output key type
   *
   * @param key The key to transform
   * @return The resulting key
   */
  OutputKey mapKey(InputKey key);

  /**
   * Transform a value to the output value type
   *
   * @param value The value to transform
   * @return The resulting value
   */
  OutputValue mapValue(InputValue value);

  @Override
  default IteratorWithResources<Map.Entry<OutputKey, OutputValue>> apply(
      IteratorWithResources<Map.Entry<InputKey, InputValue>> input) throws Exception {
    return new IteratorWithResources<Map.Entry<OutputKey, OutputValue>>() {
      @Override
      public boolean hasNext() {
        return input.hasNext();
      }

      @Override
      public Map.Entry<OutputKey, OutputValue> next() {
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
  default Map<OutputKey, OutputValue> collect(Iterator<Map.Entry<OutputKey, OutputValue>> results) {
    return IteratorTo.map(results);
  }
}
