package distroboy.core.operations;

import static java.util.stream.Collectors.groupingBy;

import distroboy.core.iterators.FlatMappingIteratorWithResources;
import distroboy.core.iterators.IteratorTo;
import distroboy.core.iterators.IteratorWithResources;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface GroupByOp<I, II extends Iterator<I>, K>
    extends Operation<II, Map.Entry<K, List<I>>, Map<K, List<I>>> {
  K classify(I output);

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
