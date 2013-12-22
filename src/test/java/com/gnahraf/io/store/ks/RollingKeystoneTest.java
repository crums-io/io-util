/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.ks;


import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * 
 * @author Babak
 */
public class RollingKeystoneTest extends KeystoneImplTest {

  @Override
  protected Keystone createKeystone(FileChannel fileChannel, long startOffset, long initValue) throws IOException {
    return new RollingKeystone(fileChannel, startOffset, initValue);
  }

  @Override
  protected Keystone loadKeystone(FileChannel fileChannel, long startOffset) throws IOException {
    return new RollingKeystone(fileChannel, startOffset);
  }
  
}
