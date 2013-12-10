/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.util.cc;

/**
 * 
 * @author Babak
 */
public enum RunState {
  
  INIT,
  STARTED,
  SUCCEEDED,
  FAILED;
  
  public boolean hasStarted() {
    return this != INIT;
  }
  
  public boolean isRunning() {
    return this == STARTED;
  }
  
  public boolean hasFinished() {
    return this == SUCCEEDED || this == FAILED;
  }
  
  
  public boolean succeeded() {
    return this == SUCCEEDED;
  }
  
  public boolean failed() {
    return this == FAILED;
  }

}
