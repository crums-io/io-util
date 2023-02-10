/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.sef;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;

import io.crums.io.channels.ChannelUtils;

/**
 * Ascending longs file.
 * Single-file implementation of a set of non-negative, ascending {@code long}s.
 * The overhead per value added is measured in bits: it is the number of
 * right-most significant bits in the value.
 * <h2>Basic Design</h2>
 * <p>
 * The ascending values are recorded in 2 places. One, their right-most significant
 * bits are appended to the end of a blob of <em>bits</em> (that is, we keep track
 * of the bit offset in a blob of bytes in a file). Two, each recorded value
 * increments the counter corresponding to its bit-width (those right-most
 * significant bits), that is, the bit-width histogram.
 * </p><p>
 * Since the values are ascending, the bit-width histogram is updated
 * left-to-right, from the low-bit bins to the hi-bit bins. This, in turn,
 * makes lookups possible in constant time. For any index, we calculate its
 * position in the bit-width histogram (which bin it incremented), from that
 * its offset in the bits blob, and retrieve the value directly.
 * </p>
 * <h2>Some Details</h2>
 * <p>
 * The bit-width histogram, a fixed length structure, is recorded at the head
 * of the file. The number of bits written per value is actually one less than
 * the value's bit-width (since if we already know the bit-width, then the
 * hi bit is redundant). The edge cases for values 0 and 1 are a bit thorny
 * under this scheme: rather than deal with these, we add 2 to every value on
 * the way in; subtract 2, on the way out.
 * </p>
 * <h2>Future Directions?</h2>
 * <p>
 * So this simplified Elias-Fano encoding uses only a single prefix bit (the
 * hi bit). Going full EF is possible. It requires a more interesting bit prefix
 * histogram. The histogram, btw, need not be linear (e.g. we might prefix encode
 * the bins for higher values differently than for lower ones). Also, since full
 * EF histograms can be quite beefy (still small enough to be held in-memory), a
 * growable version (pay-as-you-go) in its own file might make sense.
 * </p>
 */
public class Alf extends AscLongs implements Channel {
  
  
  
  private final static int HISTOGRAM_BYTES = AscBitsHistogram.FREQ_BINS * 8;
  private final static int MAX_HEADER_BYTES_ALLOC = 1024;
  
  private static AscBitsHistogram loadHistogram(
      FileChannel file, long headerBytes) throws IOException {
    
    if (headerBytes < 0)
      throw new IllegalArgumentException("negative header bytes: " + headerBytes);
    
    long len = file.size();
    var headBuffer = ByteBuffer.allocate(HISTOGRAM_BYTES);
    
    final long histogramEndOffset = headerBytes + HISTOGRAM_BYTES;
    if (len < histogramEndOffset) {
      if (len == headerBytes || len == 0 && headerBytes <= MAX_HEADER_BYTES_ALLOC) {
        // initialize and return an empty histogram
        ChannelUtils.writeRemaining(file, headerBytes, headBuffer);
        return new AscBitsHistogram();
      }
      
      throw new IllegalArgumentException(
          "underflow reading histogram bytes (expected=" + histogramEndOffset +
          " bytes; actual=" + len + "; header-bytes=" + headerBytes);
    }
    
    ChannelUtils.readRemaining(file, headerBytes, headBuffer).flip();
    long[] freq = new long[AscBitsHistogram.FREQ_BINS];
    for (int index = 0; index < freq.length; ++index)
      freq[index] = headBuffer.getLong();
    return new AscBitsHistogram(freq);
  }
  
  
  
  
  
  private int lastCommitHiIndex;
  private long lastCommitCount;
  
  
  /** Creates or loads an instance, sans the user-defined header. */
  public Alf(FileChannel file) throws IOException {
    this(file, 0L);
  }
  

  /**
   * Creates or loads a new instance.
   * 
   * @param file          empty or existing file. If empty, or only contains the optional header bytes
   *                      then it must be in "write" mode.
   * @param headerBytes   number of bytes in optional file header (&ge; 0)
   */
  public Alf(FileChannel file, long headerBytes) throws IOException {
    super(file, HISTOGRAM_BYTES + headerBytes, loadHistogram(file, headerBytes));
    lastCommitHiIndex = this.wFreq.hiIndex();
    lastCommitCount = size();
  }
  
  
  
  /** Returns the last committed size (non-negative). */
  public long lastCommitSize() {
    return lastCommitCount;
  }
  
  /** Returns the number of values that have yet to be committed. */
  public long pendingCommits() {
    return size() - lastCommitCount;
  }
  
  /** Commits the changes since the last commit. */
  public void commit() throws IOException {
    final long size = size(); // snapshot, not cuz it's expensive (it's not)
    if (size == lastCommitCount)
      return;
    
    final long hOffset = zeroOffset - HISTOGRAM_BYTES;
    final int hiIndex = wFreq.hiIndex();
    
    var buffer = ByteBuffer.allocate(HISTOGRAM_BYTES);
    for (int index = lastCommitHiIndex; index < AscBitsHistogram.FREQ_BINS; ++index)
      buffer.putLong(wFreq.freqCount(index));
    ChannelUtils.writeRemaining(
        blobFile, hOffset + lastCommitHiIndex*8, buffer.flip());
    blobFile.force(false);

    lastCommitHiIndex = hiIndex;
    lastCommitCount = size;
  }
  
  
  
  private void clearCommits() {
    lastCommitCount = lastCommitHiIndex = 0;
  }



  @Override
  public boolean isOpen() {
    return blobFile.isOpen();
  }


  /**
   * Closes the instances without committing the changes.
   * 
   * @see #close(boolean)
   */
  @Override
  public void close() throws IOException {
    close(false);
  }

  /**
   * Closes the instance, optionally committing the changes.
   * 
   * @param commit  if ({@code false} then the changes are <em>not</em> committed
   */
  public void close(boolean commit) throws IOException {
    if (commit)
      commit();
    blobFile.close();
  }
  
  
  @Override
  public void trimSize(long newSize, boolean trimFile) throws IOException {
    super.trimSize(newSize, trimFile);
    clearCommits();
    commit();
  }
  

}

















