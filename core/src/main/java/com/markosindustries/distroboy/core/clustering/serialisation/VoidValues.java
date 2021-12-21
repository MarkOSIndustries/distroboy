package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.Value;

/** Default serialiser for {@link Void}s */
public class VoidValues implements Serialiser<Void> {
  @Override
  public Value serialise(Void value) {
    return Value.getDefaultInstance();
  }

  @Override
  public Void deserialise(Value value) throws Exception {
    return null;
  }
}
