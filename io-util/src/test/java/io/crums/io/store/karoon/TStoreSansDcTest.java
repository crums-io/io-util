/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.store.karoon;

import io.crums.io.store.table.del.DeleteCodec;

/**
 * Noticing the tests assume a DeleteCodec. An application may not have one: so testing for
 * the simpler case hasn't been covered.
 */
public class TStoreSansDcTest extends TStoreTest {
  
  @Override
  protected DeleteCodec getDeleteCodec() {
    return null;
  }
  
  

}
