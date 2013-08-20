/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.ks;


import java.io.IOException;


/**
 * Caching facade over a <tt>Keystone</tt> implementation.
 * 
 * @author Babak
 */
public class CachingKeystone extends Keystone {

  private final Keystone base;

  private long currentValue;


  /**
   * Creates a new instance with the given <tt>base</tt> instance.
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

}

