package distroboy.core.operations;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public interface MapFrom {
  static <K, V> Map<K, V> iterator(Iterator<Map.Entry<K, V>> entries) {
    final var map = new HashMap<K, V>();
    while (entries.hasNext()) {
      Map.Entry<K, V> next = entries.next();
      map.put(next.getKey(), next.getValue());
    }
    return map;
  }
}
