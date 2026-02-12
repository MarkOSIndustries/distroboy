package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.Value;

/** Default serialiser for {@link String}s */
public class StringValues implements Serialiser<String> {
  @Override
  public String deserialise(Value value) throws Exception {
    return value.getStringValue();
  }

  @Override
  public Value serialise(String value) throws Exception {
    return Value.newBuilder().setStringValue(value).build();
  }
}
