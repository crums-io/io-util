/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.ks;


import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;

/**
 * A keystone maintains a long value that is typically serialized to a file.
 * Updates are atomic, all-or-nothing, and fail-safe in the face partial writes.
 * The point of this structure is its application in other fail-safe structures
 * that support higher order atomic operations.
 * 
 * @author Babak
 */
public abstract class Keystone implements Channel {
  
  
  /**
   * Creates a new instance by loading a previously serialized instance.
   * 
   * @param file
   *          open channel to the underlying file positioned at the start
   *          of the serialized keystone. On return the file's position is
   *          advanced to the byte just beyond the keystone.
   *  
   * @return a {@linkplain RollingKeystone RollingKeystone} instance
   */
  public static Keystone loadInstance(FileChannel file) throws IOException {
    long offset = file.position();
    Keystone keystone = loadInstance(file, offset);
    file.position(offset + keystone.size());
    return keystone;
  }
  
  
  /**
   * Creates a new instance by loading a previously serialized instance.
   * 
   * @param file
   *          open channel to the underlying file. The file's position is not modified.
   * @param fileOffset
   *          the offset at which this keystone begins
   *  
   * @return a {@linkplain RollingKeystone RollingKeystone} instance
   */
  public static Keystone loadInstance(FileChannel file, long offset) throws IOException {
    return new RollingKeystone(file, offset);
  }
  


  /**
   * Creates a new instance with given initial value and writes its state to
   * persistent storage. (There's no such thing as uninitialized keystone.)
   * 
   * @param file
   *          open channel to the underlying file positioned at the start
   *          of the serialized keystone. On return the file's position is
   *          advanced to the byte just beyond the keystone.
   * @param initValue
   *          the initial value of the keystone
   *  
   * @return a {@linkplain RollingKeystone RollingKeystone} instance
   */
  public static Keystone createInstance(FileChannel file, long initValue) throws IOException {
    long offset = file.position();
    Keystone keystone = createInstance(file, offset, initValue);
    file.position(offset + keystone.size());
    return keystone;
  }
  


  /**
   * Creates a new instance with given initial value and writes its state to
   * persistent storage. (There's no such thing as uninitialized keystone.)
   * 
   * @param file
   *          open channel to the underlying file. The file's position is not modified.
   * @param fileOffset
   *          the offset at which this keystone begins
   * @param initValue
   *          the initial value of the keystone
   *  
   * @return a {@linkplain RollingKeystone RollingKeystone} instance
   */
  public static Keystone createInstance(FileChannel file, long offset, long initValue) throws IOException {
    return new RollingKeystone(file, offset, initValue);
  }
  
  

  /**
   * Returns the byte-width of this keystone structure.
   */
  public abstract int size();


  /**
   * Returns the current value.
   */
  public abstract long get() throws IOException;


  /**
   * Puts (writes) the given value and returns the old value. This is equivalent
   * to {@linkplain #put(long, boolean) put(value, false)}.
   * 
   * @param value
   *          the new value
   * @return the old value
   */
  public long set(long value) throws IOException {
    return put(value, false);
  }


  /**
   * Puts (writes) the given value and returns the old value. This is the same
   * as {@linkplain #set(long)} except it gives the user the option to half the
   * number of forces to the backing storage device, if this method is to be
   * invoked very many times in succession in a batch job.
   * 
   * @param value
   *          the new value
   * @param rollingCommit
   *          if <tt>true</tt>, then the final force to the keystone byte is
   *          omitted. This halves the number of forces to the file system,
   *          at the cost of the possible loss of the last put; in no event
   *          (er, never say that) will the keystone's value be corrupt with
   *          this parameter set to <tt>true</tt>
   * @return the old value
   * @return
   * @throws IOException
   */
  public abstract long put(long value, boolean rollingCommit) throws IOException;


  /**
   * Increments the current value and returns the new incremented value.
   * 
   * @param delta
   *          the amount to be incremented
   * @return the new, incremented value
   */
  public long increment(long delta) throws IOException {
    return increment(delta, false);
  }


  /**
   * Increments the current value and returns the new incremented value. This is
   * the same as {@linkplain #increment(long)} except it gives the user the
   * option to half the number of forces to the backing storage device, if this
   * method is to be invoked very many times in succession in a batch job.
   * 
   * @param delta
   *          the amount to be incremented
   * @param rollingCommit
   *          if <tt>true</tt>, then the final force to the keystone byte is
   *          omitted.
   * @return the new, incremented value
   */
  public abstract long increment(long delta, boolean rollingCommit) throws IOException;


  /**
   * Forces a write to the underlying file. This figures in only if the last
   * write (i.e. the last invocation of either of the <tt>put()</tt> or
   * <tt>increment()</tt> methods) had the <tt>rollingCommit</tt> flag on.
   * 
   * @see #put(long, boolean)
   * @see #increment(long, boolean)
   */
  public abstract void commit() throws IOException;



  @Override
  public boolean isOpen() {
    return true;
  }



  @Override
  public void close() throws IOException {  }
}
