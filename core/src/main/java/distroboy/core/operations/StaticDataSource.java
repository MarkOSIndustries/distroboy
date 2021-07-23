package distroboy.core.operations;

import java.util.Collection;
import java.util.Iterator;

public class StaticDataSource<I> implements DataSource<I> {
  private final Collection<I> data;

  public StaticDataSource(Collection<I> data) {
    this.data = data;
  }

  @Override
  public long countOfFullSet() {
    return data.size();
  }

  @Override
  public Iterator<I> enumerateRangeOfFullSet(long startInclusive, long endExclusive) {
    return data.stream().skip(startInclusive).limit(endExclusive).iterator();
  }
}
