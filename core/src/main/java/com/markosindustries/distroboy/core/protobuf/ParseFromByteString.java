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
  T parseFrom(ByteString bytes) throws InvalidProtocolBufferException;
}
