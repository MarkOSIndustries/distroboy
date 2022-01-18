package com.markosindustries.distroboy.core.clustering;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PersistedData {
  private final ConcurrentMap<UUID, PersistedDataReference<?>> storedReferences =
      new ConcurrentHashMap<>();

  PersistedDataReference<?> retrieve(DataReferenceId dataReferenceId) {
    return storedReferences.get(dataReferenceId.getUUID());
  }

  public void store(
      DataReferenceId dataReferenceId, PersistedDataReference<?> persistedDataReference) {
    storedReferences.put(dataReferenceId.getUUID(), persistedDataReference);
  }
}
