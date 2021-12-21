package com.markosindustries.distroboy.parquet;

import java.util.AbstractList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.parquet.example.data.Group;

public class ParquetGroupInspector {
  private final Group group;

  public ParquetGroupInspector(Group group) {
    this.group = group;
  }

  public Group getGroup() {
    return group;
  }

  public boolean isRepeated(String fieldName) {
    return group.getFieldRepetitionCount(fieldName) > 1;
  }

  public boolean isPresent(String fieldName) {
    return group.getFieldRepetitionCount(fieldName) > 0;
  }

  public boolean isPrimitive(String fieldName) {
    return group.getType().getType(fieldName).isPrimitive();
  }

  public ParquetGroupInspector inspectGroup(String fieldName) {
    int fieldIndex = group.getType().getFieldIndex(fieldName);
    if (group.getType().getType(fieldIndex).isPrimitive()) {
      throw new RuntimeException(
          "Parquet field " + fieldName + " is a Primitive type, but was accessed as a Group type");
    }
    return new ParquetGroupInspector(group.getGroup(fieldIndex, 0));
  }

  public List<ParquetGroupInspector> inspectRepeatedGroup(String fieldName) {
    int fieldIndex = group.getType().getFieldIndex(fieldName);
    if (group.getType().getType(fieldIndex).isPrimitive()) {
      throw new RuntimeException(
          "Parquet field " + fieldName + " is a Primitive type, but was accessed as a Group type");
    }
    int valueCount = group.getFieldRepetitionCount(fieldIndex);
    return new AbstractList<>() {
      @Override
      public ParquetGroupInspector get(int index) {
        return new ParquetGroupInspector(group.getGroup(fieldIndex, index));
      }

      @Override
      public int size() {
        return valueCount;
      }
    };
  }

  public <T> T getPrimitive(
      String fieldName, Function<Group, BiFunction<Integer, Integer, T>> getPrimitiveAccessor) {
    int fieldIndex = group.getType().getFieldIndex(fieldName);
    if (!group.getType().getType(fieldIndex).isPrimitive()) {
      throw new RuntimeException(
          "Parquet field " + fieldName + " is a Group type, but was accessed as a Primitive");
    }
    return getPrimitiveAccessor.apply(group).apply(fieldIndex, 0);
  }

  public <T> List<T> getRepeatedPrimitive(
      String fieldName, Function<Group, BiFunction<Integer, Integer, T>> getPrimitiveAccessor) {
    int fieldIndex = group.getType().getFieldIndex(fieldName);
    if (!group.getType().getType(fieldIndex).isPrimitive()) {
      throw new RuntimeException(
          "Parquet field " + fieldName + " is a Group type, but was accessed as a Primitive");
    }
    int valueCount = group.getFieldRepetitionCount(fieldIndex);
    return new AbstractList<>() {
      @Override
      public T get(int index) {
        return getPrimitiveAccessor.apply(group).apply(fieldIndex, index);
      }

      @Override
      public int size() {
        return valueCount;
      }
    };
  }
}
