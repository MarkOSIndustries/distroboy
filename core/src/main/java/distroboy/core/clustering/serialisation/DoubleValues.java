package distroboy.core.clustering.serialisation;

import distroboy.schemas.Value;

public class DoubleValues implements Serialiser<Double> {
  @Override
  public Value serialise(Double value) {
    return Value.newBuilder().setDoubleValue(value).build();
  }

  @Override
  public Double deserialise(Value value) throws Exception {
    return value.getDoubleValue();
  }
}
