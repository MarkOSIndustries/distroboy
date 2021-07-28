package distroboy.core.operations;

import distroboy.core.iterators.IteratorWithResources;
import java.util.Collection;

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
  public IteratorWithResources<I> enumerateRangeOfFullSet(long startInclusive, long endExclusive) {
    return IteratorWithResources.from(
        data.stream().skip(startInclusive).limit(endExclusive).iterator());
  }
}
