package com.markosindustries.distroboy.core.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * An interface to represent the parseFrom function all protobuf generated classes support
 *
 * @param <T> The type of protobuf returned
 */
@FunctionalInterface
public interface ParseFromByteArray<T extends MessageLite> {
  T parseFrom(byte[] bytes) throws InvalidProtocolBufferException;
}
