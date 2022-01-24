package com.markosindustries.distroboy.core.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * An iterator for a file written using protobuf's varint-length-prefixed format
 *
 * @param <T> The type of protobuf records in the file
 */
public class ProtobufFileIterator<T extends MessageLite> implements IteratorWithResources<T> {
  private final DataInputStream input;
  private final ParseFromByteArray<T> parseFrom;
  private boolean hasNext;
  private int nextMessageSize;

  /**
   * An iterator for a file written using protobuf's varint-length-prefixed format
   *
   * @param file The file containing the records
   * @param parseFrom The protobuf type's parseFrom method to deserialise the records
   * @throws FileNotFoundException If the file doesn't exist
   */
  public ProtobufFileIterator(final File file, ParseFromByteArray<T> parseFrom)
      throws FileNotFoundException {
    this.input = new DataInputStream(new FileInputStream(file));
    this.parseFrom = parseFrom;
    this.hasNext = true;
    this.nextMessageSize = tryGetNextMessageSize();
  }

  private int tryGetNextMessageSize() {
    try {
      final var nextFirstByte = input.read();
      if (nextFirstByte == -1) {
        hasNext = false;
        return -1;
      } else {
        return CodedInputStream.readRawVarint32(nextFirstByte, input);
      }
    } catch (Exception ex) {
      hasNext = false;
      return -1;
    }
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public T next() {
    final var bytes = new byte[nextMessageSize];
    try {
      input.readFully(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    nextMessageSize = tryGetNextMessageSize();
    try {
      return parseFrom.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    input.close();
  }
}
