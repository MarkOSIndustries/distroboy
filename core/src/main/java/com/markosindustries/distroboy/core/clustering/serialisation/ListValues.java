package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.RepeatedValue;
import com.markosindustries.distroboy.schemas.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ListValues<T> implements Serialiser<List<T>> {
  private final Serialiser<T> serialiser;

  public ListValues(Serialiser<T> serialiser) {
    this.serialiser = serialiser;
  }

  @Override
  public Value serialise(List<T> value) {
    return Value.newBuilder()
        .setRepeatedValue(
            RepeatedValue.newBuilder()
                .addAllValues(
                    value.stream()
                        .map(serialiser::serialise)
                        .collect(Collectors.toUnmodifiableList()))
                .build())
        .build();
  }

  @Override
  public List<T> deserialise(Value value) throws Exception {
    final var list = new ArrayList<T>(value.getRepeatedValue().getValuesList().size());
    for (Value v : value.getRepeatedValue().getValuesList()) {
      list.add(serialiser.deserialise(v));
    }
    return list;
  }
}
