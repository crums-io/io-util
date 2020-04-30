/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.ks;

import java.io.IOException;

/**
 * An immutable keystone. The mutator methods throw <tt>UnsupportedOperationException</tt>.
 * 
 * @author Babak
 */
public final class FixedKeystone extends Keystone {
  
  private final long value;
  
  public FixedKeystone(long value) {
    this.value = value;
  }

  @Override
  public int size() {
    return 0;
  }


  @Override
  public long get() throws IOException {
    return value;
  }


  @Override
  public long put(long value, boolean rollingCommit) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }


  @Override
  public long increment(long delta, boolean rollingCommit) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }


  @Override
  public void commit() {
  }

}
