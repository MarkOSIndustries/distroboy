package com.markosindustries.distroboy.core.iterators;

import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MappingIteratorTest {
  @Test
  public void shouldResultInSameNumberOfItems() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var actual =
        (Iterable<Integer>) () -> new MappingIterator<>(list.iterator(), Function.identity());
    Assertions.assertIterableEquals(list, actual);
  }

  @Test
  public void shouldTransformItemsUsingSuppliedMapper() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var actual = (Iterable<Integer>) () -> new MappingIterator<>(list.iterator(), x -> x + 1);
    Assertions.assertIterableEquals(List.of(2, 3, 4, 5, 6, 7, 8), actual);
  }

  @Test
  public void shouldThrowIfIteratorIsNull() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new MappingIterator<>(null, Function.identity()));
  }

  @Test
  public void shouldThrowIfMapperIsNull() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new MappingIterator<>(list.iterator(), null));
  }
}
