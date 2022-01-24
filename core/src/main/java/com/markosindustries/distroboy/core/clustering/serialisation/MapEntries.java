package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.RepeatedValue;
import com.markosindustries.distroboy.schemas.Value;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

/** Default serialiser for {@link Map.Entry}s */
public class MapEntries<K, V> implements Serialiser<Map.Entry<K, V>> {
  private final Serialiser<K> keySerialiser;
  private final Serialiser<V> valueSerialiser;

  /**
   * Create a MapEntries serialiser
   *
   * @param keySerialiser The serialiser for the key type the maps contain
   * @param valueSerialiser The serialiser for the value type the maps contain
   */
  public MapEntries(Serialiser<K> keySerialiser, Serialiser<V> valueSerialiser) {
    this.keySerialiser = keySerialiser;
    this.valueSerialiser = valueSerialiser;
  }

  @Override
  public Value serialise(Map.Entry<K, V> value) {
    return Value.newBuilder()
        .setRepeatedValue(
            RepeatedValue.newBuilder()
                .addAllValues(
                    List.of(
                        keySerialiser.serialise(value.getKey()),
                        valueSerialiser.serialise(value.getValue()))))
        .build();
  }

  @Override
  public Map.Entry<K, V> deserialise(Value value) throws Exception {
    return new AbstractMap.SimpleImmutableEntry<>(
        keySerialiser.deserialise(value.getRepeatedValue().getValuesList().get(0)),
        valueSerialiser.deserialise(value.getRepeatedValue().getValuesList().get(1)));
  }
}
