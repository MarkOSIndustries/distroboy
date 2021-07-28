package distroboy.core.clustering.serialisation;

import distroboy.schemas.Value;

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
