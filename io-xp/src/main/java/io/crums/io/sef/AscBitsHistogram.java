/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.sef;

import java.util.Arrays;
import java.util.Objects;

import io.crums.util.Lists;

/**
 * Bit-width histogram for non-negative (&ge; 2, actually), strictly
 * increasing numbers (hereafter, <em>value</em>s). It's responsible for 2 things:
 * <ol>
 * <li> Track the occurances of values by their bit-width.</li>
 * <li> Calculate the <em>bit</em> offset of the location the value at an index is
 *      recorded.
 * </li>
 * </ol>
 * <h2>Limits</h2>
 * <p>
 * There are a number of quirks with this class (why it's package-private).
 * In particular, it doesn't handle starting values &lt 2. (This is so that
 * the bit serialization of every value has an implicit leading 1-bit.)
 * But since a starting value of zero is common, we'll be just adding 2
 * to every value on the write-path, subtracting 2 on the read-path.
 * </p><p>
 * Also, since this class's "API" uses 8-byte, signed {@code long}s to communicate
 * bit offsets, the maximum {@linkplain #size() size} of the histogram is somewhere
 * less than 2<sup>60</sup>. (Note this about the <em>total number of values</em> in the histogram,
 * not the values themselves.) This code does not check whether this limit is breached
 * without assertions turned on.
 * </p>
 * 
 * @see #MIN_VALUE
 */
class AscBitsHistogram {
  
  /** Minimum value has 2 bits. Simplifies logic. */
  public final static long MIN_VALUE = 2;
  /**
   * Total number of bins in the histogram. The first bin index ([0])
   * corresponds to 2-bit values, the second ([1]), to 3-bit values,
   * and so on.
   */
  public final static int FREQ_BINS = 62;
  
  private final long[] freq = new long[FREQ_BINS];
  private final long[] cumuFreq = new long[FREQ_BINS];
  private final long[] cumuBits = new long[FREQ_BINS];
  
  /** The highest valid index. */
  private int hiIndex;
  
  
  
  /**
   * Creates an empty instance.
   */
  public AscBitsHistogram() {  }
  
  
  /**
   * Deep copy constructor. State is initialized to {@code copy}
   * but is otherwise independent.
   */
  public AscBitsHistogram(AscBitsHistogram copy) {
    for (int index = freq.length; index-- > 0; ) {
      freq[index] = copy.freq[index];
      cumuFreq[index] = copy.cumuFreq[index];
      cumuBits[index] = copy.cumuBits[index];
    }
    hiIndex = copy.hiIndex;
  }
  

  /**
   * 
   */
  public AscBitsHistogram(long[] freq) {
    reset(freq);
  }
  
  
  
  
  private void reset(long[] freq) {
    if (freq.length != this.freq.length)
      throw new IllegalArgumentException(
          "wrong number of elements (" + freq.length +
          ") in frequency array; expected " + this.freq.length);

    long count = 0;
    long bitCount = 0;
    hiIndex = 0;
    
    for (int index = 0; index < this.freq.length; ++index) {
      long f = freq[index];
      if (f < 0)
        throw new IllegalArgumentException(
            "negative frequency (" + f + ") at [" + index + "]: " +
            Lists.longList(freq));
      
      long max = maxCountForIndex(index);
      if (f > max)
        throw new IllegalArgumentException(
            "out-of-bounds frequency (" + f + ") at [" + index + "] (max is " +
            max + "): " + Lists.longList(freq));
      this.freq[index] = f;
      count += f;
      bitCount += f * (1 + index);  // # bit-width = 2 + index; leading bit dropped
      this.cumuFreq[index] = count;
      this.cumuBits[index] = bitCount;
      if (f != 0)
        hiIndex = index;
    }
  }
  
  
  
  public boolean trimSize(long newSize) {
    long size = size();
    if (newSize == size)
      return false;
    
    // return true beyond here
    
    if (newSize > size)
      throw new IllegalArgumentException(
          "newSize %d > size %d".formatted(newSize, size));
    if (size < 0)
      throw new IllegalArgumentException("newSize: " + newSize);
    
    if (size == 0) {
      
      for (int index = freq.length; index-- > 0;)
        freq[index] = 0;
    
    } else {
    
      int binIndex = binIndex(newSize - 1);
      long toRemove = size - newSize;
      for (int index = freq.length - 1; index > binIndex; --index) {
        toRemove -= freq[index];
        freq[index] = 0;
      }
      freq[binIndex] -= toRemove;
      assert freq[binIndex] >= 0;
    }
    
    reset(freq);
    return true;
  }
  
  
  
  /**
   * Adds the given non-negative (increasing) value and returns the <em>bit</em>
   * offset at which the number is to be written. Since the instance doesn't actually
   * track the numbers, it can only detect out-of-sequence input if the bit-width of the
   * {@code number} argument is less than that of the last input. (As a sanity check,
   * it also detects if too many numbers are added for a given bit-width.)
   * 
   * @param value &ge; {@linkplain #MIN_VALUE}
   * 
   * @return the bit offset the number is to be recorded at
   */
  public long addNext(long value) {
    if (value < MIN_VALUE)
      throw new IllegalArgumentException(value + " < min value " + MIN_VALUE);
    
    final int bitWidth = (byte) (64 - Long.numberOfLeadingZeros(value));
    return addNextValues(bitWidth, 1);
//    final int freqIndex = bitWidth - 2;
//    
//    if (freqIndex == hiIndex) {
//      
//      long nextCount = ++freq[hiIndex];
//      long max = maxCountForIndex(hiIndex);
//      
//      if (nextCount > max) {
//        --freq[hiIndex];
//        throw new IllegalArgumentException(
//            "detected attempt to add more numbers than are possible in a bit width of " +
//            bitWidth + "; number = " + value);
//      }
//      
//    } else if (freqIndex > hiIndex) {
//      
//      final int oldHiIndex = hiIndex;
//      hiIndex = freqIndex;
//      for (int index = oldHiIndex + 1; index <= hiIndex; ++index) {
//        cumuFreq[index] = cumuFreq[oldHiIndex];
//        cumuBits[index] = cumuBits[oldHiIndex];
//      }
//      freq[hiIndex]++;
//      
//    } else
//      // freqIndex < hiFreqIndex
//      throw new IllegalArgumentException(
//          "attempt to add " + value + " to back of histogram; hiIndex = " + hiIndex);
//    
//    // freq[hiIndex] is already updated
//    ++cumuFreq[hiIndex];
//    long bitOffset = cumuBits[hiIndex];
//    cumuBits[hiIndex] += bitWidth - 1;
//    return bitOffset;
  }
  
  
  
  /**
   * Adds {@code count}-many "values" of the specified bit-width.
   * 
   * @param bitWidth  &ge; {@linkplain #lastBitWidth()} and &le; 63
   * @param count     &ge; 1
   * 
   * @return the bit-offset the start of the sequence is located
   */
  public long addNextValues(int bitWidth, int count) {
    if (count <= 0) 
      throw new IllegalArgumentException("count: " + count);

    final int freqIndex = bitWidth - 2;
    
    if (freqIndex == hiIndex) {
      
      long nextCount = freq[hiIndex] + count;
      long max = maxCountForIndex(hiIndex);
      
      if (nextCount > max) {
        throw new IllegalArgumentException(
            "overflow for bit-width " + bitWidth + "; " + count + " + " +
            freq[hiIndex] + " > max " + max);
      }
      
      freq[hiIndex] = nextCount;
      
    } else if (freqIndex > hiIndex) {
      
      long max = maxCountForIndex(freqIndex);
      if (count > max)
        throw new IllegalArgumentException(
            "attempt to add more items with bit-width " + bitWidth +
            "; count " + count + " > max " +  max);
      
      for (int index = hiIndex + 1; index <= freqIndex; ++index) {
        cumuFreq[index] = cumuFreq[hiIndex];
        cumuBits[index] = cumuBits[hiIndex];
      }
      hiIndex = freqIndex;
      freq[hiIndex] = count;
      
    } else
      
      throw new IllegalArgumentException(
          "attempt to add to back of histogram (?): bit-width " + bitWidth +
          "; current minimum bit-width is " + (hiIndex + 2));
    
    cumuFreq[hiIndex] += count;
    
    long bitOffset = cumuBits[hiIndex];
    cumuBits[hiIndex] = bitOffset + (bitWidth - 1) * count;
    assert cumuBits[hiIndex] > 0;
    return bitOffset;
  }
  
  
  
  
  /**
   * Returns the bit-offset where the value for number at the given {@code index}
   * is located. Other information is communicated thru the {@code wout} parameter.
   * 
   * @param index   index of the number, 0 &le; {@code index} &lt; {@code size()}
   * @param wout    out param hack. [0]: bit-length
   * 
   * @return the bit-offset; other info is written to {@code wout}
   */
  public long bitOffset(long index, byte[] wout) throws IndexOutOfBoundsException {
    Objects.checkIndex(index, size());
    // we want to set cumuIndex to the frequency bin *index* is registered in
    final int cumuIndex = binIndex(index);
    
    long bitOffset = cumuIndex == 0 ? 0 : cumuBits[cumuIndex - 1];
    
    long indexInFreqBin = cumuIndex == 0 ? index : index - cumuFreq[cumuIndex - 1];
    byte bitLen = blobValueBits(cumuIndex);
    bitOffset += bitLen * indexInFreqBin;
    wout[0] = bitLen;
    assert bitOffset >= 0;
    
    return bitOffset;
  }
  
  
  
  private int binIndex(long index) {
    long count = index + 1;   // (recall, the histogram records counts)
    

    int searchIdx = Arrays.binarySearch(cumuFreq, 0, hiIndex + 1, count);
    if (searchIdx < 0)
      return -1 - searchIdx;
    else {
      while (searchIdx > 0 && cumuFreq[searchIdx -1] == cumuFreq[searchIdx])
        searchIdx--;
      return searchIdx;
    }
  }
  
  
  /**
   *  Recall bit-widths are 2 greater than {@code freqIndex} (bit-width 2 goes in slot 0).
   *  However, the hi-bit in the blob file is implicit (not written), so the blob-bits are
   *  just 1 greater (not 2) than the bin index.
   *  
   *  @see #MIN_VALUE
   */
  private byte blobValueBits(int freqIndex) {
    return (byte) (freqIndex + 1);
  }
  
  
  private long maxCountForIndex(int index) {
    // index 0 is for 2 bit values
    return maxCountForWidth(index + 2); 
  }
  
  private long maxCountForWidth(int bitWidth) {
    // not valid for bit-widths less than 2, but we don't allow those (0 and 1).
    return 1L << (bitWidth - 1);
  }
  
  
  /**
   * Returns the frequency count for values with the given bit-width.
   * 
   * @param bitWidth the bit-width of the values recorded (not their serialized representation, which is 1 bit less)
   * 
   * @throws IllegalArgumentException
   *         if {@code bitWidth} is outside the range {@code [2,63]} (inc)
   */
  public long freqCountByBitwidth(int bitWidth) {
    checkBitWidthBounds(bitWidth);
    return freq[bitWidth - 2];
  }
  
  
  /**
   * Returns the count for the given bits-width bin.
   * 
   * @param index   bits-bin index (2 less than the bit-width); 0 &le; {@code index} &lt; 62
   * @return
   */
  public long freqCount(int index) throws IndexOutOfBoundsException {
    return freq[index];
  }
  
   
  private void checkBitWidthBounds(int bitWidth) {
    if (bitWidth < 2 || bitWidth > 63)
      throw new IllegalArgumentException("bit-width out of bounds: " + bitWidth);
  }
  
  /** Returns the max count the given {@code bitWidth} can have. */
  public long maxCountForBitWidth(int bitWidth) {
    checkBitWidthBounds(bitWidth);
    return maxCountForBitWidth(bitWidth);
  }
  
  
  /**
   * Returns the index of the current bin. (Recall bins are filled left-to-right).
   * 
   * @return &ge; 0 and &lt; 62
   */
  public int hiIndex() {
    return hiIndex;
  }
  
  
  /**
   * Returns the total number of values recorded in the histogram.
   * 
   * @return &ge; 0
   */
  public long size() {
    return cumuFreq[hiIndex];
  }
  
  
  /** Returns the blob-file's frontier bit-offset. */
  public long blobBits() {
    return cumuBits[hiIndex];
  }
  
  /** Returns the blob-file's frontier byte-offset. */
  public long blobBytes() {
    return (blobBits() + 7) / 8;
  }
  
  /**
   * Returns the frontier bit-width.
   * @return &ge; 2
   */
  public int lastBitWidth() {
    return hiIndex + 2;
  }

}

















