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
 * @param <I> The type of the input data set items
 * @param <II> The type of an {@link java.util.Iterator} of the input data set items
 * @param <K> The type of the keys {@link #classify} generates
 */
public interface GroupByOp<I, II extends Iterator<I>, K>
    extends Operation<II, Map.Entry<K, List<I>>, Map<K, List<I>>> {
  /**
   * Given an input, derive a key used to group items with the same key together
   *
   * @param input The value to derive a key for
   * @return They key corresponding to the given input
   */
  K classify(I input);

  @Override
  default IteratorWithResources<Map.Entry<K, List<I>>> apply(IteratorWithResources<II> input)
      throws Exception {
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
  default Map<K, List<I>> collect(Iterator<Map.Entry<K, List<I>>> results) {
    return IteratorTo.map(results);
  }
}
