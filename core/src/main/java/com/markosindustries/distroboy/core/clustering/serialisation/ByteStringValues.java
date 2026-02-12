package com.markosindustries.distroboy.core.clustering.serialisation;

import com.google.protobuf.ByteString;
import com.markosindustries.distroboy.schemas.Value;

/** Default serialiser for byte arrays */
public class ByteStringValues implements Serialiser<ByteString> {
  @Override
  public Value serialise(ByteString value) throws Exception {
    return Value.newBuilder().setBytesValue(value).build();
  }

  @Override
  public ByteString deserialise(Value value) throws Exception {
    return value.getBytesValue();
  }
}
