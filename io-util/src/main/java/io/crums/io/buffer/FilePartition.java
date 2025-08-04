/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.io.buffer;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.crums.io.SerialFormatException;
import io.crums.io.channels.ChannelUtils;

/**
 * A partition defined inside a file. Partitions are self-delimiting
 * and a file may contain multiple partitions; regardless, each instance
 * of this class encapsulates <em>one</em> partition in a file.
 */
public class FilePartition extends Partition {
  
  /** Maximum number of parts in any given partition. */
  public final static int MAX_PARTS = 0xff_ff_ff;
  
  private FileChannel ch;
  
  private long[] offsets;

  
  /**
   * Creates a new instance over the given file channel at the
   * specified offset.
   * 
   * @param ch          file channel (not closed on failure)
   * @param offset      starting offset of partition in file
   * 
   * @throws SerialFormatException
   *         if partition header data is malformed (or if the
   *         file length is too small)
   * @throws UncheckedIOException
   *         if an I/O error occurs
   */
  public FilePartition(FileChannel ch, long offset)
      throws SerialFormatException, UncheckedIOException {
    
    this.ch = ch;
    var work = ByteBuffer.allocate(4096);
    long pos = offset;
    
    try {
      
      final int count =
          ChannelUtils.readRemaining(ch, pos, work.limit(4)).flip().getInt();
      
      pos += 4;
      
      final int sizeofSizeArray = count * 4;
      
      if (count > MAX_PARTS)
        throw new SerialFormatException(
            "read too-large part-count (%d > %d) at offset %d"
            .formatted(count, MAX_PARTS, offset));
      
      
      if (count < 0 || sizeofSizeArray > ch.size() - pos)
        throw new SerialFormatException(
            "read part-count %d with %d bytes required beyond offset %d"
            .formatted(0xff_ff_ff_ffL & count, sizeofSizeArray, pos));
      
      this.offsets = new long[count + 1];
      // assert this.offsets[0] == 0L;
      
      if (count == 0)
        return;
      
      if (work.capacity() < sizeofSizeArray)
        work = ByteBuffer.allocate(sizeofSizeArray);
      else
        work.clear().limit(sizeofSizeArray);
      
      ChannelUtils.readRemaining(ch, pos, work).flip();
      
      this.offsets[0] = pos + sizeofSizeArray;
      
      for (int index = 0; index < count; ++index) {
        int size = work.getInt();
        if (size < 0)
          throw new IOException(
              "read negative size (%d) for part %d (at offset %d)"
              .formatted(size, index, pos + index * 4));
        offsets[index + 1] = size + offsets[index];
      }
      
      if (offsets[count] > ch.size())
        throw new SerialFormatException(
            "required minimum file size %d (%d parts); actual file size is %d"
            .formatted(offsets[count], count, ch.size()));
      
      
            
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

  @Override
  public int getParts() {
    return offsets.length - 1;
  }

  @Override
  public int getPartSize(int index) throws IndexOutOfBoundsException {
    return (int) (offsets[index + 1] - offsets[index]);
  }
  

  /**
   * Reads and returns the part at the specified index.
   */
  @Override
  public ByteBuffer getPart(int index) throws IndexOutOfBoundsException {
    int size = getPartSize(index);
    if (size == 0)
      return BufferUtils.NULL_BUFFER;
    
    var part = ByteBuffer.allocate(size);
    try {
      return ChannelUtils.readRemaining(ch, offsets[index], part).flip();
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on getPart(%d): %s".formatted(index, iox), iox);
    }
  }

}
