/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.ks;

import java.io.IOException;

/**
 * 
 * @author Babak
 */
public class VolatileKeystone extends Keystone {
  
  private long value;

  public VolatileKeystone(long initValue) {
    value = initValue;
  }


  /**
   * @return zero. Instances of this class are not designed to be serialized to storage.
   */
  @Override
  public int size() {
    return 0;
  }


  @Override
  public synchronized long get() throws IOException {
    return value;
  }


  @Override
  public synchronized long put(long value, boolean rollingCommit) throws IOException {
    long oldValue = value;
    this.value = value;
    return oldValue;
  }


  @Override
  public synchronized long increment(long delta, boolean rollingCommit) throws IOException {
    return value += delta;
  }


  @Override
  public void commit() throws IOException {
  }

}
