package com.markosindustries.distroboy.core.iterators;

import com.markosindustries.distroboy.core.iterators.mock.MockResource;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MappingIteratorWithResourcesTest {
  @Test
  public void shouldResultInSameNumberOfItems() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var actual =
        (Iterable<Integer>)
            () ->
                new MappingIteratorWithResources<>(
                    IteratorWithResources.from(list.iterator()), Function.identity());
    Assertions.assertIterableEquals(list, actual);
  }

  @Test
  public void shouldTransformItemsUsingSuppliedMapper() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var actual =
        (Iterable<Integer>)
            () ->
                new MappingIteratorWithResources<>(
                    IteratorWithResources.from(list.iterator()), x -> x + 1);
    Assertions.assertIterableEquals(List.of(2, 3, 4, 5, 6, 7, 8), actual);
  }

  @Test
  public void shouldThrowIfIteratorIsNull() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new MappingIteratorWithResources<>(null, Function.identity()));
  }

  @Test
  public void shouldThrowIfMapperIsNull() {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new MappingIteratorWithResources<>(IteratorWithResources.from(list.iterator()), null));
  }

  @Test
  public void shouldCloseWrappedIteratorWhenClosed() throws Exception {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);

    final var resource = new MockResource();
    final var wrapped = IteratorWithResources.from(list.iterator(), resource);
    final var mappingIterator = new MappingIteratorWithResources<>(wrapped, Function.identity());

    Assertions.assertFalse(resource.isClosed);
    mappingIterator.close();
    Assertions.assertTrue(resource.isClosed);
  }

  @Test
  public void shouldCloseAttachedResourcesWhenIterationCompletes() throws Exception {
    final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);
    final List<MockResource> resources = List.of(new MockResource(), new MockResource());

    final var mappingIterator =
        new MappingIteratorWithResources<>(
            IteratorWithResources.from(list.iterator()), Function.identity(), resources);

    Assertions.assertFalse(resources.get(0).isClosed);
    Assertions.assertFalse(resources.get(1).isClosed);

    IteratorTo.list(mappingIterator);

    Assertions.assertTrue(resources.get(0).isClosed);
    Assertions.assertTrue(resources.get(1).isClosed);
  }
}
