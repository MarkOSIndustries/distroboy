package distroboy.core.clustering.serialisation;

import distroboy.schemas.Value;

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
