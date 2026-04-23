package com.markosindustries.distroboy.core.operations;

import static java.util.stream.Collectors.groupingBy;

import com.markosindustries.distroboy.core.iterators.FlatMappingIteratorWithResources;
import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A high level interface for any operation which takes a distributed data set and groups items in
 * it together by using the given {@link #classify} function to derive a key which can be hashed,
 * used to distribute keys with the same hash to the same node, and then on each node grouping the
 * items by the same key.
 *
 * @param <Input> The type of the input data set items
 * @param <InputIterator> The type of an {@link java.util.Iterator} of the input data set items
 * @param <Key> The type of the keys {@link #classify} generates
 */
public interface GroupByOp<Input, InputIterator extends Iterator<Input>, Key>
    extends Operation<InputIterator, Map.Entry<Key, List<Input>>, Map<Key, List<Input>>> {
  /**
   * Given an input, derive a key used to group items with the same key together
   *
   * @param input The value to derive a key for
   * @return They key corresponding to the given input
   */
  Key classify(Input input);

  @Override
  default IteratorWithResources<Map.Entry<Key, List<Input>>> apply(
      IteratorWithResources<InputIterator> input) throws Exception {
    return new FlatMappingIteratorWithResources<>(
        input,
        iterator -> {
          return IteratorWithResources.from(
              IteratorTo.list(iterator).stream()
                  .collect(groupingBy(this::classify))
                  .entrySet()
                  .iterator());
        });
  }

  @Override
  default Map<Key, List<Input>> collect(Iterator<Map.Entry<Key, List<Input>>> results) {
    return IteratorTo.map(results);
  }
}
