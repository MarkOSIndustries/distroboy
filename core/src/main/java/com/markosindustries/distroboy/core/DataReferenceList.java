package com.markosindustries.distroboy.core;

import com.markosindustries.distroboy.schemas.DataReference;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a {@link List} of {@link DataReference}s, with information supplied via the generic
 * type parameter describing the type of data being referenced
 *
 * @param <T> The type of the values at these references
 */
public class DataReferenceList<T> implements Iterable<DataReference> {
  private final List<DataReference> dataReferences;

  /**
   * Represents a {@link List} of {@link DataReference}s, with information supplied via the generic
   * type parameter describing the type of data being referenced
   *
   * @param dataReferences The list of data references
   */
  public DataReferenceList(List<DataReference> dataReferences) {
    this.dataReferences = dataReferences;
  }

  @Override
  public Iterator<DataReference> iterator() {
    return dataReferences.iterator();
  }

  /**
   * Return the list of {@link DataReference}s - stripping type information in the process
   *
   * @return The untyped list of {@link DataReference}s
   */
  public List<DataReference> list() {
    return dataReferences;
  }
}
