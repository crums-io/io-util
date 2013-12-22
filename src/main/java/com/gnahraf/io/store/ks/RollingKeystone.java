/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.ks;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Manages reliable persistence of an 8 byte value to an I/O device. This is a slight
 * improvement on {@linkplain KeystoneImpl}'s 2 cell design: it uses 3 cells in order
 * to better lock down the keystone's behavior under rolling commits: it closes a
 * data corruption vulnerability to abnormal shutdown when the calling code invokes
 * either of {@linkplain #increment(long, boolean) increment(value, false)} or
 * {@linkplain #put(long, boolean) put(value, false)} (i.e. with the <tt>rollingCommit</tt>
 * flag set to <tt>true</tt>) in succession (without having had forced a interleaving
 * sync).
 * 
 * <h4>Note</h4>
 * I'm making this the standard type of persisted keystone:
 * the 2 cell keystone (the base class) may still have its uses, but I made
 * this the default implementation in {@linkplain Keystone#createInstance(FileChannel, long)}
 * and {@linkplain Keystone#createInstance(FileChannel, long, long)}.
 * 
 * 
 * <p/>
 * 
 * <h3>File Format</h3>
 * 
 * This is serialized as a 25 byte sequence: three 8 byte wide
 * cells followed by a special, byte-wide index cell. On the read path, the
 * entire block is loaded, the index cell is consulted, and the relevant value
 * of the indexed cell is returned.
 * <p/>
 * On the write path, the new value is first written to the next cell
 * (determined in a round robin scheme), then the file is flushed, and finally
 * the index cell is updated to point to the cell with the newly written value.
 * 
 * <pre>
 * <tt>
 *  
 * 
 *     cell                        cell index (byte-size keystone)
 *   _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 *  |    8    |         |         | |
 *  |  bytes  |         |         | |
 *   - - - - - - - - - - - - - - - -
 * 
 *  ^                             ^
 *  |                             |
 *  |        O F F S E T S        |
 *  |                             |
 *  |                             |
 *   `--- fileOffset               `--- fileOffset + 24
 * 
 * </tt>
 * </pre>
 * 
 * 
 * 
 * @author Babak
 */
public class RollingKeystone extends KeystoneImpl {

  /**
   * Creates a new instance by loading a previously serialized instance.
   * 
   * @param file
   *          open channel to the underlying file
   * @param fileOffset
   *          the offset at which this keystone begins
   */
  public RollingKeystone(FileChannel file, long fileOffset) throws IOException {
    super(file, fileOffset);
  }

  /**
   * Creates a new instance with given initial value and writes its state to
   * persistent storage. (There's no such thing as uninitialized keystone.)
   * 
   * @param file
   *          open channel to the underlying file
   * @param fileOffset
   *          the offset at which this keystone begins
   * @param initValue
   *          the initial value of the keystone
   */
  public RollingKeystone(FileChannel file, long fileOffset, long initValue) throws IOException {
    super(file, fileOffset, initValue);
  }

  /**
   * @return 3
   */
  protected int cellCount() {
    return 3;
  }

}
