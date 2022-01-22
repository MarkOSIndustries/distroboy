package com.markosindustries.distroboy.core;

import com.markosindustries.distroboy.schemas.DataReference;
import java.util.Iterator;
import java.util.List;

public class DataReferenceList<T> implements Iterable<DataReference> {
  private final List<DataReference> dataReferences;

  public DataReferenceList(List<DataReference> dataReferences) {
    this.dataReferences = dataReferences;
  }

  @Override
  public Iterator<DataReference> iterator() {
    return dataReferences.iterator();
  }

  public List<DataReference> list() {
    return dataReferences;
  }
}
