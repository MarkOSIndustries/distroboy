package distroboy.core.operations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public interface ListFrom {
  static <T> List<T> iterator(Iterator<T> it) {
    final var list = new ArrayList<T>();
    while (it.hasNext()) {
      list.add(it.next());
    }
    return list;
  }
}
