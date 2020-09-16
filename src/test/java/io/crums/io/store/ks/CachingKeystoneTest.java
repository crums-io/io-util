/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.ks;


import java.io.IOException;
import java.nio.channels.FileChannel;

import io.crums.io.store.ks.CachingKeystone;
import io.crums.io.store.ks.Keystone;

/**
 * 
 * @author Babak
 */
public class CachingKeystoneTest extends KeystoneImplTest {

  @Override
  protected Keystone createKeystone(FileChannel file, long startOffset, long initValue) throws IOException {
    return new CachingKeystone(super.createKeystone(file, startOffset, initValue));
  }

  @Override
  protected Keystone loadKeystone(FileChannel file, long startOffset) throws IOException {
    return new CachingKeystone(super.loadKeystone(file, startOffset));
  }

}

