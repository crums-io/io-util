/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.iter;

/**
 * 
 * @author Babak
 */
public enum Direction {
  FORWARD,
  REVERSE;
  
  public int effectiveComp(int comp) {
    return this == FORWARD ? comp : -comp;
  }
}
