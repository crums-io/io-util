/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.ks;


import java.io.IOException;

import io.crums.io.IoStateException;


/**
 * Caching facade over a <code>Keystone</code> implementation.
 */
public class CachingKeystone extends Keystone {

  private final Keystone base;

  private long currentValue;


  /**
   * Creates a new instance with the given <code>base</code> instance.
   */
  public CachingKeystone(Keystone base) throws IOException {
    if (base == null)
      throw new IllegalArgumentException("null base");
    this.base = base;
    this.currentValue = base.get();
  }


  @Override
  public int size() {
    return base.size();
  }


  /**
   * @return the already cached value
   */
  @Override
  public synchronized long get() {
    return currentValue;
  }


  @Override
  public synchronized long put(long value, boolean rollingCommit) throws IOException {
    long oldValue = base.put(value, rollingCommit);
    if (oldValue != currentValue)
      throw new IoStateException(
          "expected old value " + currentValue + "; actual was " + oldValue);
    currentValue = value;
    return oldValue;
  }


  @Override
  public synchronized long increment(long delta, boolean rollingCommit) throws IOException {
    currentValue = base.increment(delta, rollingCommit);
    return currentValue;
  }


  @Override
  public void commit() throws IOException {
    base.commit();
  }


  @Override
  public boolean isOpen() {
    return base.isOpen();
  }


  @Override
  public void close() throws IOException {
    base.close();
  }

}

