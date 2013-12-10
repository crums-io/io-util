/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.ks;

import java.io.IOException;

/**
 * 
 * @author Babak
 */
public class FixedKeystone extends Keystone {
  
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
  public long put(long value, boolean rollingCommit) {
    throw new UnsupportedOperationException();
  }


  @Override
  public long increment(long delta, boolean rollingCommit) {
    throw new UnsupportedOperationException();
  }


  @Override
  public void commit() {
  }

}
