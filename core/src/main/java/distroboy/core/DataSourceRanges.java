package distroboy.core;

import distroboy.schemas.DataSourceRange;
import java.util.ArrayList;

public interface DataSourceRanges {
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

  static String describeRange(DataSourceRange dataSourceRange) {
    return "["
        + dataSourceRange.getStartInclusive()
        + ","
        + dataSourceRange.getEndExclusive()
        + ")";
  }
}
