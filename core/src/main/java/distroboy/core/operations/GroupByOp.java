package distroboy.core.operations;

import static java.util.stream.Collectors.groupingBy;

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
    return MapFrom.iterator(results);
  }
}
