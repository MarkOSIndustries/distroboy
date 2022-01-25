package com.markosindustries.distroboy.core.clustering;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Management class for data being made available to other members of the cluster. */
public class DistributableData {
  private final ConcurrentMap<UUID, DistributableDataReference<?>> storedReferences =
      new ConcurrentHashMap<>();

  /**
   * Return the actual data reference identified
   *
   * @param dataReferenceId The Id of the {@link DistributableDataReference} to retrieve
   * @return The requested {@link DistributableDataReference}
   */
  DistributableDataReference<?> retrieve(DataReferenceId dataReferenceId) {
    final var distributableDataReference = storedReferences.get(dataReferenceId.getUUID());
    if (!distributableDataReference.isReiterable()) {
      storedReferences.remove(dataReferenceId.getUUID());
    }
    return distributableDataReference;
  }

  /**
   * Store a {@link DistributableDataReference} under the given Id
   *
   * @param dataReferenceId The Id of the data reference
   * @param distributableDataReference The actual distributable data reference
   */
  public void add(
      DataReferenceId dataReferenceId, DistributableDataReference<?> distributableDataReference) {
    storedReferences.put(dataReferenceId.getUUID(), distributableDataReference);
  }
}
