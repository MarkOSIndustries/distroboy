package com.markosindustries.distroboy.core.iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Provides static helper methods for converting {@link Iterator}s to other types */
public interface IteratorTo {
  /**
   * Convert an {@link IteratorWithResources} to a {@link List} by iterating through it
   *
   * @param <T> The type of values in the iterator
   * @param it The iterator to produce a list from
   * @return A {@link List} containing the values
   * @throws Exception if calling {@link IteratorWithResources#close()} fails
   */
  static <T> List<T> list(IteratorWithResources<T> it) throws Exception {
    try (it) {
      return list((Iterator<T>) it);
    }
  }

  /**
   * Convert an {@link Iterator} to a {@link List} by iterating through it
   *
   * @param <T> The type of values in the iterator
   * @param it The iterator to produce a list from
   * @return A {@link List} containing the values
   */
  static <T> List<T> list(Iterator<T> it) {
    final var list = new ArrayList<T>();
    while (it.hasNext()) {
      list.add(it.next());
    }
    return list;
  }

  /**
   * Convert an {@link Iterator} of {@link Map.Entry}s to a {@link Map} by iterating through it
   *
   * @param <K> The type of keys in the iterator's {@link Map.Entry}s
   * @param <V> The type of values in the iterator's {@link Map.Entry}s
   * @param entries The iterator to produce a map from
   * @return A {@link Map} containing the entries
   */
  static <K, V> Map<K, V> map(Iterator<Map.Entry<K, V>> entries) {
    final var map = new HashMap<K, V>();
    while (entries.hasNext()) {
      Map.Entry<K, V> next = entries.next();
      map.put(next.getKey(), next.getValue());
    }
    return map;
  }
}
