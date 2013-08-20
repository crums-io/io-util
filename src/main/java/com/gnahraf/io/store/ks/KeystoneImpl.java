/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.ks;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

import com.gnahraf.io.channels.ChannelUtils;


/**
 * Manages reliable persistence of an 8 byte value to an I/O device. In
 * particular, a keystone updates values in a way that is resilient to partial
 * writes (say in the face of a power failure). Indeed, that's the whole point
 * of this thing.
 * <p/>
 * 
 * <h3>File Format</h3>
 * 
 * A keystone is serialized as a 17 byte sequence: two 8 byte wide
 * cells followed by a special, byte-wide index cell. On the read path, the
 * entire block is loaded, the index cell is consulted, and the relevant value
 * of the indexed cell is returned.
 * <p/>
 * On the write path, the new value is first written to the next cell
 * (determined by a a round robin scheme), then the file is flushed, and finally
 * the index cell is updated to point to the cell with the newly written value.
 * 
 * <pre>
 * <tt>
 *  
 * 
 *     cell              cell index (byte-size keystone)
 *   _ _ _ _ _ _ _ _ _ _ _
 *  |    8    |         | |
 *  |  bytes  |         | |
 *   - - - - - - - - - - -
 * 
 *  ^                   ^
 *  |                   |
 *  |   O F F S E T S   |
 *  |                   |
 *  |                   |
 *   ` fileOffset        ` fileOffset + 16
 * 
 * </tt>
 * </pre>
 * 
 * 
 * <h3>A note on the cell size</h3>
 * 
 * It's possible to generalize this design to return anything, not just longs.
 * For now, this is good enough for me.
 * 
 * @author Babak
 */
public class KeystoneImpl extends Keystone {
  
  private final static int STRUC_SIZE = 17;

  private final long fileOffset;
  private final ByteBuffer workBuffer;

  private final FileChannel file;




  /**
   * Creates a new instance by loading a previously serialized instance.
   * 
   * @param file
   *          open channel to the undelying file
   * @param fileOffset
   *          the offset at which this keystone begins
   * @param cellCount
   *          the number of cells in the keystone (between 2 and 256). (This
   *          number is not represented anywhere in the file format.)
   */
  public KeystoneImpl(FileChannel file, long fileOffset) throws IOException {
    // construct the instance and validate parameters..
    this(file, fileOffset, false);

    if (file.size() < fileOffset + workBuffer.capacity())
      throw new IOException("No keystone found at offset " + fileOffset + "; file is " + file.size() + " bytes");
  }


  /**
   * Creates a new instance with given initial value and writes its state to
   * persistent storage. (There's no such thing as uninitialized keystone.)
   * 
   * @param file
   *          open channel to the undelying file
   * @param fileOffset
   *          the offset at which this keystone begins
   * @param cellCount
   *          the number of cells in the keystone (between 2 and 256). (This
   *          number is not represented anywhere in the file format.)
   * @param initValue
   *          the initial value of the keystone
   */
  public KeystoneImpl(FileChannel file, long fileOffset, long initValue) throws IOException {
    // construct the instance and validate parameters
    this(file, fileOffset, false);

    // initialize the keystone value
    workBuffer.clear();
    workBuffer.putLong(initValue).putLong(0).put((byte) 0);
    workBuffer.flip();
    ChannelUtils.writeRemaining(file, fileOffset, workBuffer);

    // sanity check..
    long recordedValue = get();
    if (recordedValue != initValue)
      throw new IOException("Sanity check failed: expected to read initValue <" + initValue + ">; actual was <" + recordedValue + ">");
  }
  



  private KeystoneImpl(FileChannel file, long fileOffset, boolean sigDisambiguator) throws IOException {
    if (file == null)
      throw new IllegalArgumentException("null file channel");
    if (!file.isOpen())
      throw new ClosedChannelException();
    if (fileOffset < 0)
      throw new IllegalArgumentException("fileOffset: " + fileOffset);

    this.file = file;
    this.fileOffset = fileOffset;

    this.workBuffer = ByteBuffer.allocate(STRUC_SIZE);
  }


  @Override
  public int size() {
    return STRUC_SIZE;
  }


  @Override
  public synchronized long get() throws IOException {
    workBuffer.clear();
    ChannelUtils.readRemaining(file, fileOffset, workBuffer);
    workBuffer.flip();

    int cellOffset = 8 * cellIndex();
    return workBuffer.getLong(cellOffset);
  }


  @Override
  public synchronized long put(long value, boolean rollingCommit) throws IOException {
    return putImpl(value, false, rollingCommit);
  }


  @Override
  public synchronized long increment(long delta, boolean rollingCommit) throws IOException {
    return putImpl(delta, true, rollingCommit);
  }


  @Override
  public void commit() throws IOException {
    file.force(false);
  }


  /**
   * Returns the zero-based cell index.
   * 
   * @return
   */
  private int cellIndex() {
    return 0xff & workBuffer.get(workBuffer.capacity() - 1);
  }


  private long putImpl(long value, boolean delta, boolean rollingCommit) throws IOException {
    final long oldValue = get();

    final long newValue, retValue;
    if (delta) {
      newValue = oldValue + value;
      retValue = newValue;
    } else {
      newValue = value;
      retValue = oldValue;
    }

    final int cellIndex = (1 + cellIndex()) % 2;
    workBuffer.clear();
    workBuffer.putLong(newValue).flip();

    long cellOffsetInFile = fileOffset + 8 * cellIndex;
    ChannelUtils.writeRemaining(file, cellOffsetInFile, workBuffer);
    file.force(false);

    workBuffer.clear();
    workBuffer.put((byte) cellIndex).flip();

    long byteKeystoneOffset = fileOffset + 16;
    ChannelUtils.writeRemaining(file, byteKeystoneOffset, workBuffer);

    if (!rollingCommit)
      file.force(false);

    return retValue;
  }

}
