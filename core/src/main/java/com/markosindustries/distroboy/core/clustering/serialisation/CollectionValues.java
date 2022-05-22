package com.markosindustries.distroboy.core.clustering.serialisation;

import com.markosindustries.distroboy.schemas.RepeatedValue;
import com.markosindustries.distroboy.schemas.Value;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Default serialiser for {@link Collection}s */
public class CollectionValues<T, C extends Collection<T>> implements Serialiser<C> {
  private final Function<Integer, C> newCollectionWithCapacity;
  private final Serialiser<T> serialiser;

  /**
   * Create a ListValues serialiser
   *
   * @param serialiser The serialiser for the type the lists contain
   */
  public CollectionValues(
      Function<Integer, C> newCollectionWithCapacity, Serialiser<T> serialiser) {
    this.newCollectionWithCapacity = newCollectionWithCapacity;
    this.serialiser = serialiser;
  }

  @Override
  public Value serialise(C value) {
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
  public C deserialise(Value value) throws Exception {
    final var collection =
        newCollectionWithCapacity.apply(value.getRepeatedValue().getValuesList().size());
    for (Value v : value.getRepeatedValue().getValuesList()) {
      collection.add(serialiser.deserialise(v));
    }
    return collection;
  }
}
