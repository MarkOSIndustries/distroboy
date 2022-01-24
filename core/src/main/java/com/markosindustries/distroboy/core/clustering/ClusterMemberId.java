package com.markosindustries.distroboy.core.clustering;

import com.google.protobuf.ByteString;
import java.util.Objects;
import java.util.UUID;

/**
 * An identifier for a member of the cluster. Used to identify cluster members when distributing
 * references to their data.
 */
public final class ClusterMemberId {
  private final UUID uuid;

  /**
   * An identifier for a member of the cluster. Used to identify cluster members when distributing
   * references to their data.
   */
  public ClusterMemberId() {
    this.uuid = UUID.randomUUID();
  }

  private ClusterMemberId(UUID uuid) {
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
   * Deserialise a {@link ByteString} to the equivalent {@link ClusterMemberId}
   *
   * @param bytes The bytes representing the Id
   * @return The deserialised Id
   */
  public static ClusterMemberId fromBytes(ByteString bytes) {
    return new ClusterMemberId(UUIDs.fromBytes(bytes));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ClusterMemberId that = (ClusterMemberId) o;
    return uuid.equals(that.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid);
  }

  @Override
  public String toString() {
    return "db-cm-" + uuid;
  }
}
