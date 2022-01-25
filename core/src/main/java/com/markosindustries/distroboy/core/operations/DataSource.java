package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a place data can be loaded from for a {@link DistributedOpSequence}. The only
 * requirements are that
 *
 * <ul>
 *   <li>each node in the cluster will see the same data set
 *   <li>we know how many items are in the data set
 *   <li>we can load a requested index range of the data
 * </ul>
 *
 * @param <I> The type of items in the data source
 */
public interface DataSource<I> extends Operand<I, List<I>> {
  /**
   * Determine the number of values in this data source. <b>IMPORTANT:</b> Must be consistent when
   * called from each cluster member.
   *
   * @return The number of values
   */
  long countOfFullSet();

  /**
   * Return an {@link IteratorWithResources} over the requested range of values in this data source.
   * <b>IMPORTANT:</b> Must be consistent when called from each cluster member.
   *
   * @param startInclusive The start of the range (inclusive - the value at this index will be
   *     included)
   * @param endExclusive The end of the range (exclusive - the value at this index will be omitted)
   * @return An iterator providing values in the specified range
   */
  IteratorWithResources<I> enumerateRangeOfFullSet(
      final long startInclusive, final long endExclusive);

  /**
   * Returns an empty list - a {@link DataSource} is always at the top of the chain of operations
   *
   * @return An empty list
   */
  @Override
  default List<Operand<?, ?>> dependencies() {
    return Collections.emptyList();
  }

  /**
   * Return an {@link IteratorWithResources} over the requested range of values in this data source.
   * <b>IMPORTANT:</b> Must be consistent when called from each cluster member.
   *
   * @param dataSourceRange The range of values to enumerate
   * @return An iterator providing values in the specified range
   */
  default IteratorWithResources<I> enumerateRangeForNode(final DataSourceRange dataSourceRange) {
    return enumerateRangeOfFullSet(
        dataSourceRange.getStartInclusive(), dataSourceRange.getEndExclusive());
  }

  /**
   * Given an iterator over the values in this {@link DataSource}, collect them into a {@link List}
   * <br>
   * <b>WARNING:</b> - You likely don't want to do this.. no operations have been applied to the
   * data yet, and applying operations to the values in the {@link DataSource} while the data is
   * still distributed is the whole point of distributed processing ;)
   *
   * @param results The results of iterating all elements in the {@link DataSource}
   * @return A list containing the elements in the {@link DataSource}
   */
  @Override
  default List<I> collect(final Iterator<I> results) {
    return IteratorTo.list(results);
  }
}
