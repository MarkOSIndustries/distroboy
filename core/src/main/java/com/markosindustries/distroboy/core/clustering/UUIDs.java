package com.markosindustries.distroboy.core.clustering;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.UUID;

/** A set of methods for serialisation of UUIDs */
public interface UUIDs {
  /**
   * Serialise the given UUID
   *
   * @param uuid The UUID to serialise
   * @return The {@link ByteString} representing the UUID
   */
  static ByteString asBytes(UUID uuid) {
    return ByteString.copyFrom(
        ByteBuffer.allocate(16)
            .putLong(uuid.getMostSignificantBits())
            .putLong(uuid.getLeastSignificantBits())
            .array());
  }

  /**
   * Deserialise the given UUID
   *
   * @param bytes The bytes representing the UUID
   * @return The deserialised {@link UUID}
   */
  static UUID fromBytes(ByteString bytes) {
    final var byteBuffer = bytes.asReadOnlyByteBuffer();
    final var msb = byteBuffer.getLong();
    final var lsb = byteBuffer.getLong();
    return new UUID(msb, lsb);
  }
}
