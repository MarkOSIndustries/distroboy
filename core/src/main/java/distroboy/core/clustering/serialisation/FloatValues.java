package distroboy.core.clustering.serialisation;

import distroboy.schemas.Value;

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
