package distroboy.core.clustering;

import distroboy.core.iterators.IteratorWithResources;
import distroboy.schemas.DataSourceRange;
import distroboy.schemas.Value;

public interface Job {
  IteratorWithResources<Value> execute(DataSourceRange dataSourceRange) throws Exception;
}
