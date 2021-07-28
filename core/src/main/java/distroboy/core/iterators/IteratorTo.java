package distroboy.core.iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface IteratorTo {
  static <T> List<T> list(IteratorWithResources<T> it) throws Exception {
    try (it) {
      return list((Iterator<T>) it);
    }
  }

  static <T> List<T> list(Iterator<T> it) {
    final var list = new ArrayList<T>();
    while (it.hasNext()) {
      list.add(it.next());
    }
    return list;
  }

  static <K, V> Map<K, V> map(Iterator<Map.Entry<K, V>> entries) {
    final var map = new HashMap<K, V>();
    while (entries.hasNext()) {
      Map.Entry<K, V> next = entries.next();
      map.put(next.getKey(), next.getValue());
    }
    return map;
  }
}
