package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.Value;

/** Default serialiser for {@link Long}s */
public class LongValues implements Serialiser<Long> {
  @Override
  public Long deserialise(Value value) {
    return value.getLongValue();
  }

  @Override
  public Value serialise(Long value) {
    return Value.newBuilder().setLongValue(value).build();
  }
}
