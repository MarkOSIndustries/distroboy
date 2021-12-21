package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.Value;

public class StringValues implements Serialiser<String> {
  @Override
  public String deserialise(Value value) {
    return value.getStringValue();
  }

  @Override
  public Value serialise(String value) {
    return Value.newBuilder().setStringValue(value).build();
  }
}
