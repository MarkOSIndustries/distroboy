package distroboy.core.clustering.serialisation;

import distroboy.schemas.Value;

public class StringValues implements Serialiser<String> {
  @Override
  public String deserialise(Value value) {
    return value.getStringValue();
  }

  @Override
  public Value serialise(String value) {
    return Value.newBuilder().setStringValue(value).build();
  }
}
