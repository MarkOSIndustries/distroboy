package com.markosindustries.distroboy.core.iterators;

import com.google.common.collect.Iterators;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlatMappingIteratorTest {
  @Test
  public void shouldResultInSameNumberOfItems() {
    final List<Iterator<Integer>> list =
        List.of(Iterators.forArray(1, 2), Iterators.forArray(3, 4));

    final var actual =
        (Iterable<Integer>) () -> new FlatMappingIterator<>(list.iterator(), Function.identity());
    Assertions.assertIterableEquals(List.of(1, 2, 3, 4), actual);
  }

  @Test
  public void shouldTransformItemsUsingSuppliedMapper() {
    final List<Integer[]> list = List.of(new Integer[] {1, 2}, new Integer[] {3, 4});

    final var actual =
        (Iterable<Integer>) () -> new FlatMappingIterator<>(list.iterator(), Iterators::forArray);
    Assertions.assertIterableEquals(List.of(1, 2, 3, 4), actual);
  }

  @Test
  public void shouldHandleEmptyIterators() {
    final List<Integer[]> list = List.of(new Integer[] {}, new Integer[] {});

    final var actual =
        (Iterable<Integer>) () -> new FlatMappingIterator<>(list.iterator(), Iterators::forArray);
    Assertions.assertIterableEquals(Collections.emptyList(), actual);
  }

  @Test
  public void shouldThrowIfIteratorIsNull() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new FlatMappingIterator<>(null, Function.identity()));
  }

  @Test
  public void shouldThrowIfMapperIsNull() {
    final List<Integer[]> list = List.of(new Integer[] {1, 2}, new Integer[] {3, 4});

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new FlatMappingIterator<>(list.iterator(), null));
  }
}
