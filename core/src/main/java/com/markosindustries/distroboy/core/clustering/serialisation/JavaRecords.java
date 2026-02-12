package com.markosindustries.distroboy.core.clustering.serialisation;

import com.google.protobuf.ByteString;
import com.markosindustries.distroboy.schemas.Value;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Default serialiser for Java objects. Uses Java's inbuilt object serialisation functionality */
public class JavaRecords<T> implements Serialiser<T> {
  @Override
  public T deserialise(Value value) throws Exception {
    try (final var byteStringInputStream = value.getBytesValue().newInput();
        final var objectInputStream = new ObjectInputStream(byteStringInputStream)) {
      //noinspection unchecked
      return (T) objectInputStream.readObject();
    }
  }

  @Override
  public Value serialise(T value) throws Exception {
    try (final var byteStringOutputStream = ByteString.newOutput();
        final var objectOutputStream = new ObjectOutputStream(byteStringOutputStream)) {
      objectOutputStream.writeObject(value);
      return Value.newBuilder().setBytesValue(byteStringOutputStream.toByteString()).build();
    }
  }
}
