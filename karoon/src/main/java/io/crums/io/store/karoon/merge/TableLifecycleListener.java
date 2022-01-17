/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.io.store.karoon.merge;

/**
 * 
 * @author Babak
 */
public interface TableLifecycleListener {
  
  void inited(long tableId);
  
  void released(long tableId);

}
