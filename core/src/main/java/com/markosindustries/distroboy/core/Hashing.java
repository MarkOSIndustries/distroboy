package com.markosindustries.distroboy.core;

import com.google.protobuf.MessageLite;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Useful hashing functions for grouping by common types. */
public interface Hashing {
  /**
   * Hash an integer
   *
   * @param integer The integer to hash
   * @return A hash code
   */
  static int integers(int integer) {
    return com.google.common.hash.Hashing.murmur3_32().hashInt(integer).asInt();
  }

  /**
   * Hash a long
   *
   * @param along The long to hash
   * @return A hash code
   */
  static int longs(long along) {
    return com.google.common.hash.Hashing.murmur3_32().hashLong(along).asInt();
  }

  /**
   * Hash any string
   *
   * @param string The string to hash
   * @return A hash code
   */
  static int strings(String string) {
    return com.google.common.hash.Hashing.murmur3_32().hashUnencodedChars(string).asInt();
  }

  /**
   * Hash a utf8 string
   *
   * @param string The string to hash
   * @return A hash code
   */
  static int utf8Strings(String string) {
    return com.google.common.hash.Hashing.murmur3_32()
        .hashString(string, StandardCharsets.UTF_8)
        .asInt();
  }

  /**
   * Hash an array of bytes
   *
   * @param bytes The bytes to hash
   * @return A hash code
   */
  static int bytes(byte[] bytes) {
    return com.google.common.hash.Hashing.murmur3_32().hashBytes(bytes).asInt();
  }

  /**
   * Hash a ByteBuffer
   *
   * @param bytes The bytes to hash
   * @return A hash code
   */
  static int bytes(ByteBuffer bytes) {
    return com.google.common.hash.Hashing.murmur3_32().hashBytes(bytes).asInt();
  }

  /**
   * Hash a protobuf message
   *
   * @param protobuf The message to hash
   * @return A hash code
   */
  static int protobufs(MessageLite protobuf) {
    return com.google.common.hash.Hashing.murmur3_32().hashBytes(protobuf.toByteArray()).asInt();
  }
}
