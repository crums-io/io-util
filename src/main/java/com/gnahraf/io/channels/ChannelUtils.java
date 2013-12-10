/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.channels;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.gnahraf.io.EofException;


/**
 * 
 * @author Babak
 */
public class ChannelUtils {

  private final static int MAX_NOOP_TRIALS = 1024;


  private ChannelUtils() {
  }


  public static void readRemaining(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {

    int noopCountDown = MAX_NOOP_TRIALS;

    while (buffer.hasRemaining()) {
      int amountRead = channel.read(buffer);

      if (amountRead == -1)
        throw new EofException("While attempting to read " + buffer.remaining() + " more bytes");

      noopCountDown = updateNoopCountDown(noopCountDown, amountRead);
    }
  }


  public static void readRemaining(FileChannel file, long position, ByteBuffer buffer) throws IOException {

    if (position + buffer.remaining() > file.size())
      throw new EofException("Attempt to read " + buffer.remaining() + " bytes starting from position " + position + "; file size is " + file.size() + " bytes");

    int noopCountDown = MAX_NOOP_TRIALS;

    while (buffer.hasRemaining()) {
      int amountRead = file.read(buffer, position);
      position += amountRead;

      if (amountRead == -1)
        throw new EofException("While attempting to read " + buffer.remaining() + " more bytes at position " + (position + 1));

      noopCountDown = updateNoopCountDown(noopCountDown, amountRead);
    }

  }


  public static void writeRemaining(WritableByteChannel channel, ByteBuffer buffer) throws IOException {

    int noopCountDown = MAX_NOOP_TRIALS;

    while (buffer.hasRemaining()) {
      int amountWritten = channel.write(buffer);
      noopCountDown = updateNoopCountDown(noopCountDown, amountWritten);
    }
  }


  public static void writeRemaining(FileChannel file, long position, ByteBuffer buffer) throws IOException {

    int noopCountDown = MAX_NOOP_TRIALS;

    while (buffer.hasRemaining()) {
      int amountWritten = file.write(buffer, position);
      position += amountWritten;
      noopCountDown = updateNoopCountDown(noopCountDown, amountWritten);
    }
  }
  
  
  /**
   * 
   * @param file
   * @param buffers
   * @throws IOException
   */
  public static void writeRemaining(GatheringByteChannel file, ByteBuffer[] buffers) throws IOException {

    int noopCountDown = MAX_NOOP_TRIALS;
    
    int activeIndex = 0;
    while (activeIndex < buffers.length) {
      if (!buffers[activeIndex].hasRemaining()) {
        ++activeIndex;
        continue;
      }
      long bytesWritten = file.write(buffers, activeIndex, buffers.length - activeIndex);
      noopCountDown = updateNoopCountDown(noopCountDown, bytesWritten);
    }
  }
  
  
  private static int updateNoopCountDown(int noopCountDown, long bytes) throws IOException {
    if (bytes == 0) {
      --noopCountDown;
      if (noopCountDown <= 0)
        throw new IOException("I/O operation failed after " + MAX_NOOP_TRIALS + " trials");
    } else
      noopCountDown = MAX_NOOP_TRIALS;
    return noopCountDown;
  }
  
  
  


  public static ReadableByteChannel asChannel(InputStream in) throws IOException {
    ReadableByteChannel channel;
    if (in instanceof FileInputStream)
      channel = ((FileInputStream) in).getChannel();
    else
      channel = Channels.newChannel(in);
    return channel;
  }


  public static WritableByteChannel asChannel(OutputStream out) throws IOException {
    WritableByteChannel channel;
    if (out instanceof FileOutputStream)
      channel = ((FileOutputStream) out).getChannel();
    else
      channel = Channels.newChannel(out);
    return channel;
  }
  
  
  public static void transferBytes(
      FileChannel src, WritableByteChannel sink, long offset, long length)
      throws IOException {
    
    if (length <= 0) {
      if (length == 0)
        return;
      throw new IllegalArgumentException("illegal length: " + length);
    }
    
    if (offset < 0 || offset > src.size() - length)
      throw new IllegalArgumentException(
          "offset = " + offset + "; length = " + length + "; file size = " + src.size());

    int noopCountDown = MAX_NOOP_TRIALS;

    long progress = length;
    
    while (progress > 0) {
      
      long amount = src.transferTo(offset, progress, sink);
      
      // check if we didn't transfer anything..
      noopCountDown = updateNoopCountDown(noopCountDown, amount);
      if (amount == 0 && noopCountDown < 4) {
        try {
          Thread.sleep(5);
        } catch (InterruptedException ix) {
          throw new InterruptedIOException("interrupted while sleeping");
        }
      }
      
      progress -= amount;
      offset += amount;
    }
    
    if (progress != 0)
      throw new IOException("assertion failure. progress = " + progress);
  }

}
