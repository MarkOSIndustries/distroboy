package com.markosindustries.distroboy.core.clustering.serialisation;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.markosindustries.distroboy.schemas.Value;

public class ProtobufValues<T extends MessageLite> implements Serialiser<T> {
  private final ParseFrom<T> parseFrom;

  @FunctionalInterface
  public interface ParseFrom<T> {
    T parseFrom(ByteString bytes) throws InvalidProtocolBufferException;
  }

  public ProtobufValues(ParseFrom<T> parseFrom) {
    this.parseFrom = parseFrom;
  }

  @Override
  public T deserialise(Value value) throws Exception {
    return parseFrom.parseFrom(value.getBytesValue());
  }

  @Override
  public Value serialise(T value) {
    return Value.newBuilder().setBytesValue(value.toByteString()).build();
  }
}
