package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.List;

/**
 * Force each node to generate a {@link List} from the {@link java.util.Iterator} normally used
 * while processing data sets. While this is useful to ensure that preceding steps are complete for
 * all data items, it does mean the entire data set at this point must fit into working memory.
 *
 * @param <I> The type of items to materialise
 */
public class MaterialiseOp<I> implements ListOp<I, I> {
  @Override
  public IteratorWithResources<I> apply(IteratorWithResources<I> input) throws Exception {
    return IteratorWithResources.from(IteratorTo.list(input).iterator());
  }
}
