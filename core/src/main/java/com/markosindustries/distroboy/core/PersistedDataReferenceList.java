package com.markosindustries.distroboy.core;

import com.markosindustries.distroboy.schemas.DataReference;
import java.util.List;

/**
 * Represents a {@link List} of {@link DataReference}s to persisted data, with information supplied
 * via the generic type parameter describing the type of data being referenced.
 *
 * <p>All cluster members will have the same set of data references. This is intended for use in
 * starting new jobs using methods like {@link Cluster#redistributeEqually} and {@link
 * Cluster#fromPersistedData}.
 *
 * @param <T> The type of the values at these references
 */
public class PersistedDataReferenceList<T> extends DataReferenceList<T> {
  /**
   * Represents a {@link List} of {@link DataReference}s to persisted data, with information
   * supplied via the generic type parameter describing the type of data being referenced.
   *
   * @param dataReferences The list of data references. Must all be countable.
   */
  public PersistedDataReferenceList(final List<DataReference> dataReferences) {
    super(dataReferences);
    if (dataReferences.stream().anyMatch(DataReference::getUncountable)) {
      throw new IllegalArgumentException("Persisted DataReferences must be countable");
    }
  }

  /**
   * Returns the sum of the count of items in every persisted reference across all cluster nodes.
   * This method will return the same answer on all nodes, and involves no network traffic.
   *
   * @return sum of the count of all values persisted
   */
  public long getTotalCount() {
    return dataReferences.stream().mapToLong(DataReference::getCount).sum();
  }
}
