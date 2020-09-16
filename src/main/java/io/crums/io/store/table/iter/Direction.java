/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.iter;

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
