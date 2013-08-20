/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.ks;


import java.io.IOException;

import com.gnahraf.io.store.ks.CachingKeystone;
import com.gnahraf.io.store.ks.Keystone;


/**
 * 
 * @author Babak
 */
public class CachingKeystoneTest extends KeystoneImplTest {

  @Override
  protected Keystone createKeystone(String filename, long startOffset, long initValue) throws IOException {
    return new CachingKeystone(super.createKeystone(filename, startOffset, initValue));
  }

  @Override
  protected Keystone loadKeystone(String filename, long startOffset) throws IOException {
    return new CachingKeystone(super.loadKeystone(filename, startOffset));
  }

}

