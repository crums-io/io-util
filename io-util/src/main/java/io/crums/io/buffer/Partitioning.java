/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.io.buffer;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.io.channels.ChannelUtils;
import io.crums.util.Lists;

/**
 * A simple partitioning of a {@linkplain ByteBuffer} using a list
 * of sizes defining their boundaries. Supports both read/write
 * and read-only use cases.
 */
public class Partitioning extends Partition implements Serial {
  
  

  
  /**
   * Loads a read-only instance from the given buffer. On return, the position of
   * {@code serialForm} is advanced by exactly as many bytes as required to define
   * the partitioning. Note, the <em>contents of the partitions are not read or copied</em>.
   * 
   * @param serialForm the serial form is self delimiting
   * 
   * @see #writeTo(ByteBuffer)
   * @see #serialSize() amount {@code serialForm} is advanced
   */
  public static Partitioning load(ByteBuffer serialForm) throws BufferUnderflowException {
    return load(serialForm, true);
  }
  
  /**
   * Loads an instance from the given buffer. On return, the position of
   * {@code serialForm} is advanced by exactly as many bytes as required to define
   * the partitioning. Note, the <em>contents of the partitions are not read or copied</em>.
   * 
   * @param serialForm the serial form is self delimiting
   * @param readOnly if {@code false} <em>and</em> {@code serialForm} is not read-only then
   *                 then the instance will be read/write
   * 
   * @see #writeTo(ByteBuffer)
   * @see #serialSize() amount {@code serialForm} is advanced
   */
  public static Partitioning load(ByteBuffer serialForm, boolean readOnly) throws BufferUnderflowException {
    
    // begin header
    final int partCount = serialForm.getInt();
    
    if (partCount == 0)
      return NULL;
    
    if (partCount < 0)
      throw new IllegalArgumentException(
          "read negative part count: " + partCount + " from " + serialForm);
    
    ArrayList<Integer> sizes = new ArrayList<>(partCount);
    int totalSize = 0;
    
    for (int index = 0; index < partCount; ++index) {
      int sz = serialForm.getInt();
      sizes.add(sz);
      totalSize += sz;
      
      if (sz < 0)
        throw new IllegalArgumentException(
            "negative size " + sz + " at [" + index + ":" + partCount + "]");
    }
    // end header
    
    var block = BufferUtils.slice(serialForm, totalSize);
    
    if (readOnly && !block.isReadOnly())
      block = block.asReadOnlyBuffer();
    
    return new Partitioning(block, sizes);
  }
  
  
  
  
  public static int serialSize(List<Integer> sizes) {
    final int count = sizes.size();
    int tally = headerSizeForCount(count);
    for (int index = count; index-- > 0; )
      tally += sizes.get(index);
    return tally;
  }
  
  
  public static Partitioning initForWrite(ByteBuffer out, List<Integer> sizes)
      throws BufferUnderflowException {
    
    if (out.isReadOnly())
      throw new IllegalArgumentException("out buffer not writable:  " + out);
    
    int parts = sizes.size();
    out.putInt(parts);
    int totalBytes = 0;
    for (int index = 0; index < parts; ++index) {
      int partSize = sizes.get(index);
      if (partSize < 0)
        throw new IllegalArgumentException(
            "negative part size (%d) at sizes[%d]"
            .formatted(partSize, index));
      out.putInt(partSize);
      totalBytes += partSize;
    }
    ByteBuffer partitionBlock = BufferUtils.slice(out, totalBytes);
    return new Partitioning(partitionBlock, sizes);
  }
  
  
  
  /**
   * This special null member is the only instance with zero parts.
   */
  public final static Partitioning NULL = new Partitioning();
  
  
  
  
  
  // (positional state never touched)
  private final ByteBuffer block;
  
  private final List<Integer> offsets;
  
  
  private Partitioning() {
    block = BufferUtils.NULL_BUFFER;
    offsets = List.of(0);
  }
  
  
  
  
  /**
   * Constructs a non-empty instance that is a view onto the
   * given block. If it's read-only, then this instance is also read-only.
   * 
   * @param block with capacity &ge; 1 (you're mine now: position and limit don't matter)
   * @param sizes list of non-negative sizes defining the boundaries between adjacent partitions.
   *              Must sum to the {@code block.capacity()}.
   * 
   * @see #NULL
   */
  public Partitioning(ByteBuffer block, List<Integer> sizes) {
    if (Objects.requireNonNull(block, "null block").capacity() < 1)
      throw new IllegalArgumentException("block capacity less than minimum (1): " + block.capacity());
    if (Objects.requireNonNull(sizes, "null offset").isEmpty())
      throw new IllegalArgumentException("empty sizes list");
    

    
    this.block = block.duplicate().clear();
    
    Integer[] off = new Integer[sizes.size() + 1];
    off[0] = 0;
    
    for (int index = 1; index < off.length; ++index) {
      Integer size = sizes.get(index - 1);
      if (size < 0)
        throw new IllegalArgumentException("negative size " + size + " at index " + index + ": " + sizes);
      off[index] = off[index - 1] + size;
    }
    
    if (off[off.length - 1] != block.capacity())
      throw new IllegalArgumentException(
          "sizes added up to " + off[off.length - 1] + " != block capacity " + block.capacity());
    
    this.offsets = List.of(off);
  }
  
  
  protected Partitioning(Partitioning copy) {
    this.block = copy.block;
    this.offsets = copy.offsets;
  }
  
  private Partitioning(ByteBuffer block, List<Integer> offsets, boolean dummy) {
    this.block = block;
    this.offsets = offsets;
  }
  
  
  /**
   * Returns a view of the underlying block.
   * 
   * @see #isReadOnly()
   */
  public final ByteBuffer getBlock() {
    return block.duplicate();
  }
  
  
  /**
   * Returns the size of the block in bytes.
   * 
   * @return {@code getBlock().capacity()}
   */
  public final int getBlockSize() {
    return block.capacity();
  }
  
  
  
  public final boolean isEmpty() {
    return getBlockSize() == 0;
  }
  
  
  /**
   * Determines whether it is a read-only instance. This is controlled by
   * the block argument at construction.
   */
  public final boolean isReadOnly() {
    return block.isReadOnly();
  }
  
  
  /**
   * Returns a read-only view of this instance, if the instance isn't already
   * read-only; otherwise returns this instance.
   */
  public Partitioning readOnlyView() {
    return block.isReadOnly() ? this : new Partitioning(block.asReadOnlyBuffer(), offsets, false);
  }
  
  
  @Override
  public final int getParts() {
    return offsets.size() - 1;
  }
  
  
  /**
   * @return a <em>sliced</em> view of the partition
   */
  @Override
  public final ByteBuffer getPart(int index) throws IndexOutOfBoundsException {
    Objects.checkIndex(index, offsets.size() - 1);
    int pos = offsets.get(index);
    int limit = offsets.get(index + 1);
    return
        pos == limit ?
            BufferUtils.NULL_BUFFER :
              getBlock().position(pos).limit(limit).slice();
  }
  
  
  @Override
  public final int getPartSize(int index) throws IndexOutOfBoundsException {
    Objects.checkIndex(index, offsets.size() - 1);
    return offsets.get(index + 1) - offsets.get(index);
  }
  
  
  
  @Override
  public final List<ByteBuffer> asList() {
    return new ListView();
  }
  
  private class ListView extends Lists.RandomAccessList<ByteBuffer> {
    @Override
    public ByteBuffer get(int index) {
      return getPart(index);
    }
    @Override
    public int size() {
      return getParts();
    }
  }


  
  @Override
  public int serialSize() {
    return serialHeaderSize() + block.capacity();
  }
  
  /**
   * Returns the header size of the serial representation.
   */
  public int serialHeaderSize() {
    return 4 * offsets.size();
  }
  
  
  /**
   * Writes the header only to the given {@code out} buffer and returns it. The
   * position of the buffer is advanced by {@linkplain #serialHeaderSize()}.
   */
  public ByteBuffer writeHeaderTo(ByteBuffer out) {
    final int count = offsets.size() - 1;
    out.putInt(count);
    for (int index = 0; index < count; ++index)
      out.putInt(offsets.get(index + 1) - offsets.get(index));
    return out;
  }
  


  /**
   * <p>
   * Performance note: since this <em>copies</em> the block into the
   * given {@code out} buffer, this operation is best delayed or never
   * invoked.
   * </p>
   * {@inheritDoc}
   * 
   * @return {@code writeHeaderTo(out).put(getBlock())}
   * 
   * @see #writeHeaderTo(ByteBuffer)
   * 
   */
  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    writeHeaderTo(out);
    out.put(getBlock());
    return out;
  }

  /**
   * Avoids the intermediate copying of the {@linkplain #getBlock() block}.
   * 
   * @see Serial#writeTo(WritableByteChannel)
   */
  @Override
  public void writeTo(WritableByteChannel out) throws IOException {
    var header =
        writeHeaderTo(ByteBuffer.allocate(serialHeaderSize())).flip();
    ChannelUtils.writeRemaining(out, header);
    ChannelUtils.writeRemaining(out, getBlock());
  }

}

















