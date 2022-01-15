package com.markosindustries.distroboy.core.iterators;

import com.markosindustries.distroboy.core.iterators.mock.MockResource;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparseIteratorWithResourcesTest {
  @Test
  public void shouldHonourInterval() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var actual =
        (Iterable<Integer>)
            () ->
                new SparseIteratorWithResources<>(
                    IteratorWithResources.from(list.iterator()), 0, 2);
    Assertions.assertIterableEquals(List.of(1, 3, 5, 7), actual);
  }

  @Test
  public void shouldHonourStartOffset() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var actual =
        (Iterable<Integer>)
            () ->
                new SparseIteratorWithResources<>(
                    IteratorWithResources.from(list.iterator()), 4, 1);
    Assertions.assertIterableEquals(List.of(5, 6, 7), actual);
  }

  @Test
  public void shouldHonourStartOffsetAndInterval() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var actual =
        (Iterable<Integer>)
            () ->
                new SparseIteratorWithResources<>(
                    IteratorWithResources.from(list.iterator()), 4, 2);
    Assertions.assertIterableEquals(List.of(5, 7), actual);
  }

  @Test
  public void shouldThrowIfIteratorIsNull() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new SparseIteratorWithResources<>(null, 0, 1));
  }

  @Test
  public void shouldThrowIfIntervalIsZero() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new SparseIteratorWithResources<>(IteratorWithResources.from(list.iterator()), 4, 0));
  }

  @Test
  public void shouldThrowIfStartOffsetIsNegative() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new SparseIteratorWithResources<>(IteratorWithResources.from(list.iterator()), -1, 2));
  }

  @Test
  public void shouldSkipEverythingIfStartOffsetIsLargeEnough() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var actual =
        (Iterable<Integer>)
            () ->
                new SparseIteratorWithResources<>(
                    IteratorWithResources.from(list.iterator()), 7, 1);
    Assertions.assertIterableEquals(Collections.emptyList(), actual);
  }

  @Test
  public void shouldCloseWrappedIteratorWhenClosed() throws Exception {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var resource = new MockResource();
    final var wrapped = IteratorWithResources.from(list.iterator(), resource);
    final var sparseIterator = new SparseIteratorWithResources<>(wrapped, 0, 1);

    Assertions.assertFalse(resource.isClosed);
    sparseIterator.close();
    Assertions.assertTrue(resource.isClosed);
  }
}
