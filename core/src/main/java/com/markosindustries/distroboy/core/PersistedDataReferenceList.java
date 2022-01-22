package com.markosindustries.distroboy.core;

import com.markosindustries.distroboy.schemas.DataReference;
import java.util.List;

public class PersistedDataReferenceList<T> extends DataReferenceList<T> {
  public PersistedDataReferenceList(List<DataReference> dataReferences) {
    super(dataReferences);
    if (dataReferences.stream().anyMatch(DataReference::getUncountable)) {
      throw new IllegalArgumentException("Persisted DataReferences must be countable");
    }
  }
}
