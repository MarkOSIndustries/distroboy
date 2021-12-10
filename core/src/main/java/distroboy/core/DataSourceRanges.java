package distroboy.core;

import distroboy.schemas.DataSourceRange;
import java.util.ArrayList;

/**
 * Methods for working with DataSourceRange objects A DataSourceRange represents a range of data
 * from a data source. These are used to divide a source data set up into chunks (ranges) for
 * processing.
 */
public interface DataSourceRanges {
  /**
   * Generate DataSourceRanges for a given data set
   *
   * @param countOfFullSet The total data elements in the source set
   * @param requestedNumberOfRanges The number of ranges we want to generate
   * @return The requested set of DataSourceRanges
   */
  static DataSourceRange[] generateRanges(long countOfFullSet, int requestedNumberOfRanges) {
    if (requestedNumberOfRanges == 0) {
      throw new IllegalArgumentException("Can't generate zero ranges");
    }

    final var numberOfRangesPossible = (int) Math.min(countOfFullSet, requestedNumberOfRanges);
    final var naiveRangeSize =
        numberOfRangesPossible == 0 ? 0 : Math.floorDiv(countOfFullSet, numberOfRangesPossible);
    long[] rangeSizes = new long[requestedNumberOfRanges];
    for (int i = 0; i < numberOfRangesPossible; i++) {
      rangeSizes[i] = naiveRangeSize;
    }
    // Account for rounding on the floorDiv
    rangeSizes[0] += countOfFullSet - naiveRangeSize * numberOfRangesPossible;

    final var ranges = new ArrayList<DataSourceRange>(requestedNumberOfRanges);
    long nextStart = 0;
    for (int i = 0; i < requestedNumberOfRanges; i++) {
      final var nextEnd = nextStart + rangeSizes[i];
      ranges.add(
          DataSourceRange.newBuilder()
              .setStartInclusive(nextStart)
              .setEndExclusive(nextEnd)
              .build());
      nextStart = nextEnd;
    }

    if (nextStart != countOfFullSet) {
      throw new RuntimeException(
          "Range size calculation failed, " + nextStart + " != " + countOfFullSet);
    }

    return ranges.toArray(DataSourceRange[]::new);
  }

  /**
   * Describe a DataSourceRange for logging/debugging purposes
   *
   * @param dataSourceRange The DataSourceRange to describe
   * @return A string representation of the given DataSourceRange
   */
  static String describeRange(DataSourceRange dataSourceRange) {
    return "["
        + dataSourceRange.getStartInclusive()
        + ","
        + dataSourceRange.getEndExclusive()
        + ")";
  }
}
