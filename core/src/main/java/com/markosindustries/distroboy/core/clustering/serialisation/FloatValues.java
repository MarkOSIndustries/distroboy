package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.Value;

/** Default serialiser for {@link Float}s */
public class FloatValues implements Serialiser<Float> {
  @Override
  public Value serialise(Float value) throws Exception {
    return Value.newBuilder().setFloatValue(value).build();
  }

  @Override
  public Float deserialise(Value value) throws Exception {
    return value.getFloatValue();
  }
}
