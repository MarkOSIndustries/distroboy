package com.markosindustries.distroboy.core;

import com.markosindustries.distroboy.schemas.DataReference;
import java.util.List;

/**
 * Represents a {@link List} of {@link DataReference}s to persisted data, with information supplied
 * via the generic type parameter describing the type of data being referenced.
 *
 * @param <T> The type of the values at this reference
 */
public class PersistedDataReferenceList<T> extends DataReferenceList<T> {
  /**
   * Represents a {@link List} of {@link DataReference}s to persisted data, with information
   * supplied via the generic type parameter describing the type of data being referenced.
   *
   * @param dataReferences The list of data references. Must all be countable.
   */
  public PersistedDataReferenceList(List<DataReference> dataReferences) {
    super(dataReferences);
    if (dataReferences.stream().anyMatch(DataReference::getUncountable)) {
      throw new IllegalArgumentException("Persisted DataReferences must be countable");
    }
  }
}
