package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import com.markosindustries.distroboy.schemas.Value;

/**
 * Represents a job which the cluster has been asked to run. Each call to {@link
 * com.markosindustries.distroboy.core.Cluster#execute} yields a new Job.
 */
public interface Job {
  /**
   * Load a range of data from the data source, and apply the job, returning an {@link
   * IteratorWithResources} of the resulting values for the collect phase.
   *
   * @param dataSourceRange The range of values from the data source to be processed
   * @return The results of the job, to be collected on the cluster leader node
   * @throws Exception If the job fails to execute
   */
  IteratorWithResources<Value> execute(DataSourceRange dataSourceRange) throws Exception;
}
