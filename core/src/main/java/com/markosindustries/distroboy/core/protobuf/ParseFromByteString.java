package com.markosindustries.distroboy.core.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * An interface to represent the parseFrom function all protobuf generated classes support
 *
 * @param <T> The type of protobuf returned
 */
@FunctionalInterface
public interface ParseFromByteString<T extends MessageLite> {
  /**
   * Parse a {@link ByteString} as the protobuf type {@link T}
   *
   * @param bytes The {@link ByteString} containing the serialised protobuf
   * @return An instance of the protobuf type {@link T}
   * @throws InvalidProtocolBufferException If the bytes do not represent a valid protobuf record
   */
  T parseFrom(ByteString bytes) throws InvalidProtocolBufferException;
}
