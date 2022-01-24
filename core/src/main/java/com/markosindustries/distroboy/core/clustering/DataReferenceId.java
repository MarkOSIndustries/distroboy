package com.markosindustries.distroboy.core.clustering;

import com.google.protobuf.ByteString;
import java.util.Objects;
import java.util.UUID;

/**
 * An identifier for a set of data on the cluster. Used to distribute the existence of that data to
 * other cluster members
 */
public class DataReferenceId {
  private final UUID uuid;

  /**
   * An identifier for a set of data on the cluster Used to distribute the existence of that data to
   * other cluster members
   */
  public DataReferenceId() {
    this.uuid = UUID.randomUUID();
  }

  private DataReferenceId(UUID uuid) {
    this.uuid = uuid;
  }

  /**
   * Get the UUID of this Id
   *
   * @return The UUID
   */
  public UUID getUUID() {
    return uuid;
  }

  /**
   * Serialise this Id to a {@link ByteString}
   *
   * @return the serialised Id
   */
  public ByteString asBytes() {
    return UUIDs.asBytes(uuid);
  }

  /**
   * Deserialise a {@link ByteString} to the equivalent {@link DataReferenceId}
   *
   * @param bytes The bytes representing the Id
   * @return The deserialised Id
   */
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
