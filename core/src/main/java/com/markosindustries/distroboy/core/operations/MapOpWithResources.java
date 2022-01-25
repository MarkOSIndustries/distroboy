package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.ResultWithResource;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import java.util.ArrayList;

/**
 * A high level interface for any operation which takes a distributed data set and transforms each
 * item in it via the given {@link #map} function into the output value type with an associated
 * {@link AutoCloseable} resource for each output value. These associated resources will be closed
 * when iteration of results is complete.
 *
 * @param <I> The type of the input data set items
 * @param <O> The type of the output data set items
 */
@FunctionalInterface
public interface MapOpWithResources<I, O> extends ListOp<I, O> {
  /**
   * Transform an input to the output type and wrap it in a {@link ResultWithResource} including the
   * resource to be closed when iteration is complete
   *
   * @param input The value to be mapped
   * @return The result of the map operation
   */
  ResultWithResource<O> map(I input);

  @Override
  default IteratorWithResources<O> apply(IteratorWithResources<I> input) throws Exception {
    final var resources = new ArrayList<AutoCloseable>();
    return new MappingIteratorWithResources<>(
        input,
        i -> {
          final var resultWithResource = this.map(i);
          resources.add(resultWithResource.getResource());
          return resultWithResource.getResult();
        },
        resources);
  }
}
