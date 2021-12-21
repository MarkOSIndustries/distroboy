package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import com.markosindustries.distroboy.schemas.Value;

public interface Job {
  IteratorWithResources<Value> execute(DataSourceRange dataSourceRange) throws Exception;
}
