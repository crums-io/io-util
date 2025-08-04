/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.io.buffer;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import io.crums.io.Serial;
import io.crums.io.channels.ChannelUtils;
import io.crums.util.Lists;

/**
 * An abstract partitioning of bytes.
 */
public abstract class Partition {
  

  /**
   * Writes a partitioning of {@linkplain Serial} objects to memory.
   * 
   * <h4>Motivation</h4>
   * <p>
   * Since {@code Serial} objects are already encoded in self-delimiting form,
   * the information written is clearly redundant. The goal is to allow random-access,
   * lazy-loading on reading the list from memory. This construction allows random
   * access when the serial size of the objects is not fixed.
   * </p>
   * @param out     the buffer written to (starting from its current position)
   * @param objects possibly empty list of serial objects of a common type
   * 
   * @return {@code out}
   */
  public static ByteBuffer writePartition(ByteBuffer out, List<? extends Serial> objects)
      throws BufferUnderflowException {
    
    final int count = objects.size();
    out.putInt(count);
    final int sizesHeadPos = out.position();
    out.position(sizesHeadPos + 4*count);
    for (int index = 0; index < count; ++index) {
      int pos = out.position();
      objects.get(index).writeTo(out);
      int size = out.position() - pos;
      int sizePos = sizesHeadPos + 4*index;
      out.putInt(sizePos, size);
    }
    return out;
  }
  
  

  
  
  /**
   * Writes a partition composed of {@linkplain Serial} objects to a stream.
   * 
   * <h4>Motivation</h4>
   * <p>
   * Since {@code Serial} objects are already encoded in self-delimiting form,
   * the information written is clearly redundant. The goal is to allow random-access,
   * lazy-loading on reading the list from file. This construction allows random
   * access when the serial size of the objects is not fixed.
   * </p>
   * @param out     the channel written to (starting from its current position)
   * @param objects possibly empty list of serial objects of a common type
   */
  public static void writeSerialPartition(
      WritableByteChannel out, List<? extends Serial> objects) throws IOException {
    
    ByteBuffer work = ByteBuffer.allocate(4 * (1 + objects.size()));
    work.putInt(objects.size());
    int maxSize = 0;
    for (var obj : objects) {
      int partSize = obj.serialSize();
      work.putInt(partSize);
      if (partSize > maxSize)
        maxSize = partSize;
    }
    
    ChannelUtils.writeRemaining(out, work.flip());
    
    if (maxSize > work.capacity())
      work = ByteBuffer.allocate(maxSize);
    
    for (var obj : objects)
      ChannelUtils.writeRemaining(
          out,
          obj.writeTo(work.clear()).flip());
    
  }
  
  
  /**
   * Writes a list of raw bytes as a partition.
   */
  public static void writePartition(
      WritableByteChannel out, List<ByteBuffer> parts) throws IOException {
    
    final int count = parts.size();
    ByteBuffer work = ByteBuffer.allocate(4 * (1 + count));
    work.putInt(count);
    for (int index = 0; index < count; ++index)
      work.putInt(parts.get(index).remaining());
    ChannelUtils.writeRemaining(out, work.flip());
    
    for (int index = 0; index < count; ++index)
      ChannelUtils.writeRemaining(out, parts.get(index));
  }
  
  
  
  /** Returns the header byte-size for the given part-count. */
  public static int headerSizeForCount(int count) {
    if (count < 0)
      throw new IllegalArgumentException("count: " + count);
    return (count + 1) * 4;
  }

  
  Partition() {  }
  

  /**
   * Returns the number of parts (elements) in the partition.
   * 
   * @see #getPart(int)
   */
  public abstract int getParts();
  

  /**
   * Returns the number of bytes in the part at the given {@code index}.
   * 
   * @param index &ge; 0 and &lt; {@linkplain #getParts()}
   * 
   * @return &ge; 0
   * 
   * @see #getParts()
   * @see #getPart(int)
   */
  public abstract int getPartSize(int index) throws IndexOutOfBoundsException;
  

  /**
   * Returns the part at the given {@code index}.
   * 
   * @param index &ge; 0 and &lt; {@linkplain #getParts()}
   * 
   * @return a <em>sliced</em> view of the partition
   * 
   * @see #getParts()
   */
  public abstract ByteBuffer getPart(int index) throws IndexOutOfBoundsException;
  
  /**
   * Determines if the part at the given {@code index} is empty.
   * 
   * @return {@code getPartSize(index) == 0}
   * 
   * @see #getParts()
   */
  public final boolean isPartEmpty(int index) throws IndexOutOfBoundsException {
    return getPartSize(index) == 0;
  }
  
  
  /**
   * Returns {code true} iff <em>all</em> parts are empty (or if there are
   * no parts).
   */
  public boolean isEmpty() {
    int index = getParts();
    while (index-- > 0 && isPartEmpty(index));
    return index == -1;
  }
  

  /**
   * Returns a list view of this instance.
   */
  public List<ByteBuffer> asList() {
    return Lists.functorList(getParts(), this::getPart);
  }
  
  
  

}
