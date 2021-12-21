package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import java.util.Map;

public interface HashMapToListOp<K, V, O> extends ListOp<Map.Entry<K, V>, O> {
  O map(K key, V value);

  @Override
  default IteratorWithResources<O> apply(IteratorWithResources<Map.Entry<K, V>> input)
      throws Exception {
    return new MappingIteratorWithResources<>(
        input, entry -> this.map(entry.getKey(), entry.getValue()));
  }
}
