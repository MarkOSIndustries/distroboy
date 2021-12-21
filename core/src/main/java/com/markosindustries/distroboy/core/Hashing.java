package com.markosindustries.distroboy.core;

import com.google.protobuf.MessageLite;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface Hashing {
  static int integers(int integer) {
    return com.google.common.hash.Hashing.murmur3_32().hashInt(integer).asInt();
  }

  static int longs(long along) {
    return com.google.common.hash.Hashing.murmur3_32().hashLong(along).asInt();
  }

  static int strings(String string) {
    return com.google.common.hash.Hashing.murmur3_32().hashUnencodedChars(string).asInt();
  }

  static int utf8Strings(String string) {
    return com.google.common.hash.Hashing.murmur3_32()
        .hashString(string, StandardCharsets.UTF_8)
        .asInt();
  }

  static int bytes(byte[] bytes) {
    return com.google.common.hash.Hashing.murmur3_32().hashBytes(bytes).asInt();
  }

  static int bytes(ByteBuffer bytes) {
    return com.google.common.hash.Hashing.murmur3_32().hashBytes(bytes).asInt();
  }

  static int protobufs(MessageLite protobuf) {
    return com.google.common.hash.Hashing.murmur3_32().hashBytes(protobuf.toByteArray()).asInt();
  }
}
