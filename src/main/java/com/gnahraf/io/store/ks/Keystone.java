/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.ks;


import java.io.IOException;

/**
 * A keystone maintains a long value that is typically serialized to a file.
 * Updates are atomic, all-or-nothing, and fail-safe in the face partial writes.
 * The point of this structure is its application in other fail-safe structures
 * that support higher order atomic operations.
 * 
 * @author Babak
 */
public abstract class Keystone {

  /**
   * Returns the byte-width of this keystone structure.]
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
}
