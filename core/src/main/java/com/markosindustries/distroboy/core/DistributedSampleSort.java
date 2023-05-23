package com.markosindustries.distroboy.core;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public interface DistributedSampleSort {
  static <V> List<V> takeEquidistantSamples(
      final long valueCount, final Iterator<V> sortedValuesIterator, final int sampleCount) {
    Preconditions.checkArgument(
        sampleCount <= valueCount, "Can't take more samples than we have values");

    final var equalSampleDistance = Math.floorDiv(valueCount, sampleCount);
    final var leftOverSampleDistance = valueCount % sampleCount;
    final var sampleDistances = new long[sampleCount];
    for (int i = 0; i < sampleDistances.length; i++) {
      sampleDistances[i] = equalSampleDistance;
      if (i < leftOverSampleDistance) {
        // while this approach biases the remainder to the first few samples, it's not a big
        // difference at large sample distances, and not an expensive penalty with small data sets
        sampleDistances[i] += 1;
      }
    }

    final var samples = new ArrayList<V>(sampleCount);
    for (int i = 0; i < sampleCount; i++) {
      V nextSample = null;
      for (long l = 0; l < sampleDistances[i]; l++) {
        nextSample = sortedValuesIterator.next();
      }
      samples.add(nextSample);
    }
    return samples;
  }
}
