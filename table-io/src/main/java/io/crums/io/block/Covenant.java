/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.block;

/**
 * Indicates what a caller agrees <em>not</em> to do to an accompanying
 * buffer argument.
 * 
 * @author Babak
 */
public enum Covenant {
  
  /**
   * No promises. Not agreeing to anything.
   */
  NONE,
  /**
   * Wont modify the contents of the buffer. May modify the buffer's
   * offsets afterward, though.
   */
  WONT_MOD_CONTENTS,
  /**
   * Wont modify the offsets of the buffer, but may modify its
   * contents. So the only promise is that it's a stable window.
   */
  WONT_MOD_OFFSETS,
  /**
   * Wont modify the state of the buffer. 
   */
  WONT_MOD;
  
  
  
  public boolean hasAny() {
    return this != NONE;
  }
  
  public boolean wontModify() {
    return this == WONT_MOD;
  }
  
  public boolean wontModifyOffsets() {
    return this == WONT_MOD || this == WONT_MOD_OFFSETS;
  }
  
  public boolean wontModifyContents() {
    return this == WONT_MOD || this == WONT_MOD_CONTENTS;
  }
  
}
