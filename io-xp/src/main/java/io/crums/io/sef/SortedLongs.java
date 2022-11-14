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
 * Single-file implementation of a set of non-negative, ascending {@code long}s.
 */
public class SortedLongs extends AscLongs implements Channel {
  
  
  
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
  public SortedLongs(FileChannel file) throws IOException {
    this(file, 0L);
  }
  

  /**
   * Creates or loads a new instance.
   * 
   * @param file          empty or existing file. If empty, or only contains the optional header bytes
   *                      then it must be in "write" mode.
   * @param headerBytes   number of bytes in optional file header (&ge; 0)
   */
  public SortedLongs(FileChannel file, long headerBytes) throws IOException {
    super(file, HISTOGRAM_BYTES + headerBytes, loadHistogram(file, headerBytes));
    lastCommitHiIndex = this.wFreq.hiIndex();
    lastCommitCount = size();
  }
  
  
  
  /** Returns the last committed value (non-negative), or -1 if empty. */
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



  @Override
  public boolean isOpen() {
    return blobFile.isOpen();
  }



  @Override
  public void close() throws IOException {
    blobFile.close();
  }
  
  

}

















