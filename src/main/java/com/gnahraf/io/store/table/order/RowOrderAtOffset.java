/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.order;


/**
 * 
 * @author Babak
 */
public abstract class RowOrderAtOffset extends RowOrder {
  
  protected final int offset;

  protected RowOrderAtOffset(int offset) {
    if (offset < 0)
      throw new IllegalArgumentException("offset: " + offset);
    this.offset = offset;
  }
  
  public final int offset() {
    return offset;
  }
  
  @Override
  public boolean equals(Object other) {
    if (this == other)
      return true;
    else if (other == null)
      return false;
    else
      return getClass() == other.getClass() && offset == ((RowOrderAtOffset) other).offset;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() ^ offset;
  }
  
  
  /**
   * By default, a buffer's position is irrelevant to the comparison.
   * 
   * @return <tt>false</tt>
   */
  @Override
  public boolean isRelative() {
    return false;
  }

}
