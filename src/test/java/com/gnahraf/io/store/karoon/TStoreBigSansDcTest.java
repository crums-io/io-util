/*
 * Copyright 2020 Babak Farhang
 */
package com.gnahraf.io.store.karoon;


/**
 * Noticing the other tests are too small to generate sorted tables.
 * Fastest way to check these work with no DeleteCodec is this hack.
 * 
 * TODO: change config in other tests so they get a chance to generate sorted tables
 * and make this test unnecessary.
 */
public class TStoreBigSansDcTest extends TStoreBigTest {

  /**
   * 
   */
  public TStoreBigSansDcTest() {
    super();  // belaboring the obvious in unit test so you know below doesn't get overwritten
    this.deleteCodec = null;
  }

}
