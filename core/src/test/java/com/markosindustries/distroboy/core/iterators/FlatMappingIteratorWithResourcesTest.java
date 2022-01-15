package com.markosindustries.distroboy.core.iterators;

import com.markosindustries.distroboy.core.iterators.mock.MockResource;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlatMappingIteratorWithResourcesTest {
  @Test
  public void shouldResultInSameNumberOfItems() {
    final List<IteratorWithResources<Integer>> list =
        List.of(IteratorWithResources.of(1, 2), IteratorWithResources.of(3, 4));

    final var actual =
        (Iterable<Integer>)
            () ->
                new FlatMappingIteratorWithResources<>(
                    IteratorWithResources.from(list.iterator()), Function.identity());
    Assertions.assertIterableEquals(List.of(1, 2, 3, 4), actual);
  }

  @Test
  public void shouldTransformItemsUsingSuppliedMapper() {
    final List<Integer[]> list = List.of(new Integer[] {1, 2}, new Integer[] {3, 4});

    final var actual =
        (Iterable<Integer>)
            () ->
                new FlatMappingIteratorWithResources<>(
                    IteratorWithResources.from(list.iterator()), IteratorWithResources::of);
    Assertions.assertIterableEquals(List.of(1, 2, 3, 4), actual);
  }

  @Test
  public void shouldHandleEmptyIterators() {
    final List<Integer[]> list = List.of(new Integer[] {}, new Integer[] {});

    final var actual =
        (Iterable<Integer>)
            () ->
                new FlatMappingIteratorWithResources<>(
                    IteratorWithResources.from(list.iterator()), IteratorWithResources::of);
    Assertions.assertIterableEquals(Collections.emptyList(), actual);
  }

  @Test
  public void shouldThrowIfIteratorIsNull() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new FlatMappingIteratorWithResources<>(null, Function.identity()));
  }

  @Test
  public void shouldThrowIfMapperIsNull() {
    final List<Integer[]> list = List.of(new Integer[] {1, 2}, new Integer[] {3, 4});

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new FlatMappingIteratorWithResources<>(
                IteratorWithResources.from(list.iterator()), null));
  }

  @Test
  public void shouldCloseWrappedIteratorWhenClosed() throws Exception {
    final var resource1 = new MockResource();
    final var resource2 = new MockResource();
    final List<IteratorWithResources<Integer>> list =
        List.of(
            IteratorWithResources.of(resource1, 1, 2), IteratorWithResources.of(resource2, 3, 4));

    final var wrappedResource = new MockResource();
    final var wrapped = IteratorWithResources.from(list.iterator(), wrappedResource);
    final var flatMappingIterator =
        new FlatMappingIteratorWithResources<>(wrapped, Function.identity());

    Assertions.assertFalse(wrappedResource.isClosed);
    flatMappingIterator.close();
    Assertions.assertTrue(wrappedResource.isClosed);
  }

  @Test
  public void shouldCloseMappedIteratorsDuringIteration() throws Exception {
    final var resource1 = new MockResource();
    final var resource2 = new MockResource();
    final List<IteratorWithResources<Integer>> list =
        List.of(
            IteratorWithResources.of(resource1, 1, 2), IteratorWithResources.of(resource2, 3, 4));

    final var wrappedResource = new MockResource();
    final var wrapped = IteratorWithResources.from(list.iterator(), wrappedResource);
    final var flatMappingIterator =
        new FlatMappingIteratorWithResources<>(wrapped, Function.identity());

    Assertions.assertFalse(resource1.isClosed);
    Assertions.assertFalse(resource2.isClosed);
    while (flatMappingIterator.hasNext()) {
      flatMappingIterator.next();
    }
    Assertions.assertTrue(resource1.isClosed);
    Assertions.assertTrue(resource2.isClosed);
  }
}
