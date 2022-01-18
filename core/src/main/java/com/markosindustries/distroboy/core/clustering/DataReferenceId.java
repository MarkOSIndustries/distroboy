package com.markosindustries.distroboy.core.clustering;

import com.google.protobuf.ByteString;
import java.util.Objects;
import java.util.UUID;

public class DataReferenceId {
  private final UUID uuid;

  public DataReferenceId() {
    this.uuid = UUID.randomUUID();
  }

  private DataReferenceId(UUID uuid) {
    this.uuid = uuid;
  }

  public UUID getUUID() {
    return uuid;
  }

  public ByteString asBytes() {
    return UUIDs.asBytes(uuid);
  }

  public static DataReferenceId fromBytes(ByteString bytes) {
    return new DataReferenceId(UUIDs.fromBytes(bytes));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataReferenceId that = (DataReferenceId) o;
    return uuid.equals(that.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid);
  }

  @Override
  public String toString() {
    return "db-dr-" + uuid;
  }
}
