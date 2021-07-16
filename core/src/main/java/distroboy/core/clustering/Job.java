package distroboy.core.clustering;

import distroboy.schemas.DataSourceRange;
import distroboy.schemas.Value;
import java.util.Iterator;

public interface Job {
  Iterator<Value> execute(DataSourceRange dataSourceRange);
}
