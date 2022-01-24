package com.markosindustries.distroboy.core.clustering.serialisation;

import com.google.protobuf.MessageLite;
import com.markosindustries.distroboy.core.protobuf.ParseFromByteString;
import com.markosindustries.distroboy.schemas.Value;

/** Default serialiser for protobuf {@link MessageLite}s */
public class ProtobufValues<T extends MessageLite> implements Serialiser<T> {
  private final ParseFromByteString<T> parseFromByteString;

  /**
   * Default serialiser for protobuf {@link MessageLite}s
   *
   * @param parseFromByteString The parseFrom method of the specified protobuf type (eg: <code>
   *     MyProtobuf::parseFrom</code>)
   */
  public ProtobufValues(ParseFromByteString<T> parseFromByteString) {
    this.parseFromByteString = parseFromByteString;
  }

  @Override
  public T deserialise(Value value) throws Exception {
    return parseFromByteString.parseFrom(value.getBytesValue());
  }

  @Override
  public Value serialise(T value) {
    return Value.newBuilder().setBytesValue(value.toByteString()).build();
  }
}
