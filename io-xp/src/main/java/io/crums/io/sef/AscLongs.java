/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.sef;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Objects;

import io.crums.io.bp.BitBuffer;
import io.crums.io.bp.Bits;
import io.crums.io.channels.ChannelUtils;
import io.crums.util.Lists;

/**
 * Base implementation of a set of non-negative, ascending {@code long}s.
 * 
 * <h2>No Commit</h2>
 * <p>
 * This class deals with persisting the <em>blob file</em> of values; it
 * doesn't actually commit the in-memory statistics of the bit-width histogram.
 * There are 2 reasons for this:
 * </p>
 * <ol>
 * <li>Delay the decision whether to keep the histogram outside the file.</li>
 * <li>Until such time the histogram is committed, the values appended to the blob file
 * don't matter. This is <em>a good thing</em>, as it permits implementing block-commits.</li>
 * </ol>
 */
public class AscLongs {
  
  /**
   * The maximum number of bytes a serialized offset may span.
   * Since offsets are not byte aligned, and may be up to 63-bits
   * wide, this number is 8 + 1 = 9.
   */
  public final static int MAX_NUM_BYTE_SPAN = 9;
  
  
  
  

  final AscBitsHistogram wFreq;
  protected final FileChannel blobFile;
  
  protected final long zeroOffset;
  
  /** Last (highest) recorded number, or -1 if empty. */
  private long maxValue;
  
  

  
  AscLongs(
      FileChannel offsetFile, long zeroOffset, AscBitsHistogram wFreq)
      throws IOException {
    
    this.wFreq = Objects.requireNonNull(wFreq, "null offset width histogram");
    this.blobFile = Objects.requireNonNull(offsetFile, "null offset file");
    this.zeroOffset = zeroOffset;
    if (zeroOffset < 0)
      throw new IllegalArgumentException("negative zero offset: " + zeroOffset);
    
    long actualBytes = offsetFile.size();
    long minBytes = zeroOffset + wFreq.blobBytes();
    if (actualBytes < minBytes)
      throw new IllegalArgumentException(
          "expected " + minBytes + " bytes in offset file; actual is " + actualBytes);
    // set maxValue (inclusive).. also, kick the tires
    setMaxValue();
  }
  
  
  private void setMaxValue() throws IOException {
    long size = size();
    this.maxValue = size == 0 ? -1 : get(size - 1);
    
  }
  
  
  /** Returns the number of values in set. */
  public long size() {
    return wFreq.size();
  }
  

  /**
   * Trims the size (number of entries) of the collection.
   * Shorthand for {@code trimSize(newSize, true)}.
   * 
   * @see #trimSize(long, boolean)
   */
  public void trimSize(long newSize) throws IOException {
    trimSize(newSize, true);
  }
  
  /**
   * Trims the size (number of entries) of the collection, optionally
   * trimming the blob file of offsets as well. (The blob file needn't
   * be truncated.)
   * 
   * @param newSize       0 &ge; {@code newSize} &le; {@code size()}
   * @param trimFile      if {@code true}, then the blob file is truncated
   */
  public void trimSize(long newSize, boolean trimFile) throws IOException {
    boolean trimmed = wFreq.trimSize(newSize);
    if (trimFile && (trimmed || wFreq.blobBytes() + zeroOffset < blobFile.size())) {
      blobFile.truncate(wFreq.blobBytes() + zeroOffset);
    }
    setMaxValue();
  }
  
  /** Returns {@code size() == 0}. */
  public final boolean isEmpty() {
    return size() == 0;
  }
  
  public long get(long index)  throws IndexOutOfBoundsException, IOException {
    return get(index, null);
  }
  
  
  /**
   * Returns the value at the specified {@code index}.
   * 
   * @param index   0 &le; {@code index} &lt; {@linkplain #size()}
   * @param work    optional work buffer (with capacity &ge;
   *                {@linkplain #MAX_NUM_BYTE_SPAN} and a backing byte array)
   * 
   * @throws IndexOutOfBoundsException if {@code index} is out of bounds
   */
  public long get(long index, ByteBuffer work) throws IndexOutOfBoundsException, IOException {
    
    work = ensureWorkBuffer(work);
    
    final byte[] array = work.array();

    final int bitLength;
    final int startBit;
    
    // read the relevant bytes
    {
      // no bounds check on index; wFreq does it
      final long bitOffset = zeroOffset * 8 + wFreq.bitOffset(index, array);
      startBit = (int) (bitOffset & 7L);
      bitLength = work.array()[0];
      
      final long lastBitOffset = bitOffset + bitLength - 1; // (inc)
      long offset = bitOffset / 8;
      long lastOffset = lastBitOffset / 8;                  // (inc)
      int byteLength = 1 + (int) (lastOffset - offset);
      

      work.clear().limit(byteLength);
      ChannelUtils.readRemaining(blobFile, offset, work);
      
    }
    
    int remainingBits = bitLength;
    long value;
    {
      int intraByteEndBit = Math.min(8, startBit + bitLength);
      value = padNextBits(1L, array[0], startBit, intraByteEndBit);  // leftmost bit is implicit
      remainingBits -= intraByteEndBit - startBit;
    }
    
    for (int i = 1; remainingBits > 0; ++i) {
      int intraByteEndBit = Math.min(8, remainingBits);
      value = padNextBits(value, array[i], 0, intraByteEndBit);
      remainingBits -= intraByteEndBit;
    }
    
    assert remainingBits == 0;
    
    return value - AscBitsHistogram.MIN_VALUE;
  }
  
  

  /**
   * Adds the next (bigger than last) {@code value}.
   * @see #addNext(long, ByteBuffer)
   */
  public void addNext(long value) throws IOException {
    addNext(value, null);
  }
  
  
  /**
   * Adds the next (bigger than last) {@code value}.
   * 
   * @param value   the next value (bigger than the last)
   * @param work    optional work buffer (with capacity &ge;
   *                {@linkplain #MAX_NUM_BYTE_SPAN} and a backing byte array)
   */
  public void addNext(long value, ByteBuffer work) throws IOException {
    
    // check arg
    if (value <= maxValue) {
      var msg = value < 0 ?
          "negative value " + value :
            "out-of-sequence value " + value + "; current hi is " + maxValue;
      throw new IllegalArgumentException(msg);
    }
    
    work = ensureWorkBuffer(work);
    maxValue = value;
    
    final long wValue = value + AscBitsHistogram.MIN_VALUE;

    final int startBit;
    final long offset;;
    {
      long bitOffset = wFreq.addNext(wValue);
      startBit = (int) (bitOffset & 7L);
      offset = (bitOffset / 8) + zeroOffset;
    }

    // the number of bits written is 1 less than the value's "bit-width"
    int remainingBits = 63 - Long.numberOfLeadingZeros(wValue);
    

    final var array = work.array();
    final var bitBuffer = new BitBuffer(array);
    
    if (startBit != 0) {
      ChannelUtils.readRemaining(blobFile, offset, work.clear().limit(1));
      bitBuffer.bitPosition(startBit);
    }
    
    while (remainingBits > 7) {
      int start = 64 - remainingBits;
      byte b = Bits.bits(wValue, start, start + 8);
      bitBuffer.putLeft(b, 8);
      remainingBits -= 8;
    }
    
    if (remainingBits != 0) {
      assert remainingBits < 8;
      int start = 64 - remainingBits;
      byte b = Bits.bits(wValue, start, 64);
      bitBuffer.putRight(b, remainingBits);
    }
    
    ChannelUtils.writeRemaining(blobFile, offset, bitBuffer.asByteBuffer());
    
  }
  
  
  
  /**
   * Returns the collection as a lazy, ascending list of non-negative numbers.
   * The maximum size of the list is limited by the 4-byte signed representation
   * of {@code int}s.
   */
  public List<Long> asList() {
    long size = size();
    return Lists.functorList(
        size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size,
        this::getUnchecked);
  }
  
  
  
  private long getUnchecked(int index) {
    try {
      return get(index);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  
  
  /**
   * Returns {@code value} bit-shifted to the left by {@code (endBit - startBit)}
   * bits from {@code b}, the given byte.
   * 
   * @param value       value accumulated from previous bytes (zero, o.w.)
   * @param b           the byte read
   * @param startBit    inclusive. Range {@code [0,7]}
   * @param endBit      exclusive. Range {@code [1,8]}; &gt; {@code startBit}
   * @param prefixed    if {@code true} then a 1-bit is prefixed on the right
   * 
   * @return the given {@code value} padded on the right with the specifed bits
   */
  private long padNextBits(long value, byte b, int startBit, int endBit) {
    
    assert value > 0;
    
    final int bitLen = endBit - startBit;
    final int byteIncr;
    {
      int unsigned = b & 0xff;
      int delta = ((unsigned << startBit) & 0xff) >>> startBit;
      byteIncr = delta >>> (8 - endBit); // endBit [1,8]
    }
    value <<= bitLen;
    value += byteIncr;
    
    assert value > 0;
    return value;
  }
  
  
  private ByteBuffer ensureWorkBuffer(ByteBuffer work) {
      
    return
        work == null ||
        work.isReadOnly() ||
        !work.hasArray() ||
        work.arrayOffset() != 0 ||
        work.capacity() < MAX_NUM_BYTE_SPAN ?
            ByteBuffer.wrap(new byte[MAX_NUM_BYTE_SPAN]) :
              work;
  }

}
