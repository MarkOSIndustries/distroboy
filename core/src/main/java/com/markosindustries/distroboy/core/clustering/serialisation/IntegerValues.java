package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.Value;

public class IntegerValues implements Serialiser<Integer> {
  @Override
  public Integer deserialise(Value value) {
    return value.getIntValue();
  }

  @Override
  public Value serialise(Integer value) {
    return Value.newBuilder().setIntValue(value).build();
  }
}
