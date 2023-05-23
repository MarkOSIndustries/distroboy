package com.markosindustries.distroboy.core;

import static java.util.function.Predicate.not;

import com.markosindustries.distroboy.schemas.DataReference;
import java.util.List;

/**
 * Represents a {@link List} of {@link DataReference}s to sorted data, with information supplied via
 * the generic type parameter describing the type of data being referenced.
 *
 * @param <T> The type of the values at this reference
 */
public class SortedDataReferenceList<T> extends PersistedDataReferenceList<T> {
  /**
   * Represents a {@link List} of {@link DataReference}s to sorted data, with information supplied
   * via the generic type parameter describing the type of data being referenced.
   *
   * @param dataReferences The list of data references. Must all be sorted.
   */
  public SortedDataReferenceList(List<DataReference> dataReferences) {
    super(dataReferences);
    if (dataReferences.stream().anyMatch(not(DataReference::getSorted))) {
      throw new IllegalArgumentException("Sorted DataReferences must be sorted");
    }
  }
}
