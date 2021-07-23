package distroboy.core.operations;

import static java.util.stream.Collectors.groupingBy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface GroupByOp<I, II extends Iterator<I>, K>
    extends Operation<II, Map.Entry<K, List<I>>, Map<K, List<I>>> {
  K classify(I output);

  @Override
  default Iterator<Map.Entry<K, List<I>>> apply(Iterator<II> input) {
    return new FlatMapIterator<>(
        input,
        iterator -> {
          return ListFrom.iterator(iterator).stream()
              .collect(groupingBy(this::classify))
              .entrySet()
              .iterator();
        });
  }

  @Override
  default Map<K, List<I>> collect(Iterator<Map.Entry<K, List<I>>> results) {
    final var map = new HashMap<K, List<I>>();
    while (results.hasNext()) {
      Map.Entry<K, List<I>> next = results.next();
      map.put(next.getKey(), next.getValue());
    }
    return map;
  }
}
