package distroboy.core.clustering.serialisation;

import distroboy.schemas.Value;

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
