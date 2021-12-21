package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.Value;

public class FloatValues implements Serialiser<Float> {
  @Override
  public Value serialise(Float value) {
    return Value.newBuilder().setFloatValue(value).build();
  }

  @Override
  public Float deserialise(Value value) throws Exception {
    return value.getFloatValue();
  }
}
