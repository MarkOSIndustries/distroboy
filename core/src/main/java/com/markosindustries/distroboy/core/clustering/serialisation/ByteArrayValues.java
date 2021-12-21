package com.markosindustries.distroboy.core.clustering.serialisation;

import com.google.protobuf.ByteString;
import com.markosindustries.distroboy.schemas.Value;

public class ByteArrayValues implements Serialiser<byte[]> {
  @Override
  public Value serialise(byte[] value) {
    return Value.newBuilder().setBytesValue(ByteString.copyFrom(value)).build();
  }

  @Override
  public byte[] deserialise(Value value) throws Exception {
    return value.getBytesValue().toByteArray();
  }
}
