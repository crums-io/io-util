/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.ks;


import java.io.IOException;
import java.nio.channels.FileChannel;

import io.crums.io.store.ks.Keystone;
import io.crums.io.store.ks.RollingKeystone;

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
