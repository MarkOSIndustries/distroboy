package distroboy.core.operations;

import distroboy.core.ResultWithResource;
import distroboy.core.iterators.IteratorWithResources;
import distroboy.core.iterators.MappingIteratorWithResources;
import java.util.ArrayList;

@FunctionalInterface
public interface MapOpWithResources<I, O> extends ListOp<I, O> {
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
