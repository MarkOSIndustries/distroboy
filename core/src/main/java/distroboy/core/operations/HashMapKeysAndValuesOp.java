package distroboy.core.operations;

import distroboy.core.iterators.IteratorTo;
import distroboy.core.iterators.IteratorWithResources;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

public interface HashMapKeysAndValuesOp<K, V, K2, V2>
    extends Operation<Map.Entry<K, V>, Map.Entry<K2, V2>, Map<K2, V2>> {
  K2 mapKey(K key);

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
