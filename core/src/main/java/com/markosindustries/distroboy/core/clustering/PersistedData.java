package com.markosindustries.distroboy.core.clustering;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Represents a set of data persisted somewhere on the cluster. */
public class PersistedData {
  private final ConcurrentMap<UUID, PersistedDataReference<?>> storedReferences =
      new ConcurrentHashMap<>();

  /**
   * Return the actual data reference identified
   *
   * @param dataReferenceId The Id of the {@link PersistedDataReference} to retrieve
   * @return The requested {@link PersistedDataReference}
   */
  PersistedDataReference<?> retrieve(DataReferenceId dataReferenceId) {
    return storedReferences.get(dataReferenceId.getUUID());
  }

  /**
   * Store a {@link PersistedDataReference} under the given Id
   *
   * @param dataReferenceId The Id of the data reference
   * @param persistedDataReference The actual persisted data reference
   */
  public void store(
      DataReferenceId dataReferenceId, PersistedDataReference<?> persistedDataReference) {
    storedReferences.put(dataReferenceId.getUUID(), persistedDataReference);
  }
}
