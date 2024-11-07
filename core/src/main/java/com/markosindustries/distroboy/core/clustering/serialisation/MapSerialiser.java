package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.RepeatedValue;
import com.markosindustries.distroboy.schemas.Value;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class MapSerialiser<K, V, M extends Map<K, V>> implements Serialiser<M> {
  private final Function<Integer, M> newMapWithCapacity;
  private final Serialiser<K> keySerialiser;
  private final Serialiser<V> valueSerialiser;

  /**
   * Create a MapEntries serialiser
   *
   * @param keySerialiser The serialiser for the key type the maps contain
   * @param valueSerialiser The serialiser for the value type the maps contain
   */
  public MapSerialiser(
      Function<Integer, M> newMapWithCapacity,
      Serialiser<K> keySerialiser,
      Serialiser<V> valueSerialiser) {
    this.newMapWithCapacity = newMapWithCapacity;
    this.keySerialiser = keySerialiser;
    this.valueSerialiser = valueSerialiser;
  }

  @Override
  public Value serialise(M value) {
    return Value.newBuilder()
        .setRepeatedValue(
            RepeatedValue.newBuilder()
                .addAllValues(
                    value.entrySet().stream()
                        .flatMap(
                            v ->
                                Stream.of(
                                    keySerialiser.serialise(v.getKey()),
                                    valueSerialiser.serialise(v.getValue())))
                        .toList()))
        .build();
  }

  @Override
  public M deserialise(Value value) throws Exception {
    final var keysAndValues = value.getRepeatedValue().getValuesList();
    final var result = newMapWithCapacity.apply(keysAndValues.size() / 2);
    for (int i = 0; i < keysAndValues.size(); i += 2) {
      result.put(
          keySerialiser.deserialise(keysAndValues.get(i)),
          valueSerialiser.deserialise(keysAndValues.get(i + 1)));
    }

    return result;
  }
}
