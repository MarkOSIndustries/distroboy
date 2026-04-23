package com.markosindustries.distroboy.core.operations;

/**
 * Transform the keys for each item in the data set (leaving the values alone)
 *
 * @param <InputKey> The type of input Map keys
 * @param <Value> The type of input Map values
 * @param <OutputKey> The type of output Map keys
 */
@FunctionalInterface
public interface HashMapKeysOp<InputKey, Value, OutputKey>
    extends HashMapKeysAndValuesOp<InputKey, Value, OutputKey, Value> {
  default Value mapValue(Value value) {
    return value;
  }
}
