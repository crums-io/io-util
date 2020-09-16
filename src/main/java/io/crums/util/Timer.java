/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;

/**
 * Timer itch.
 */
public final class Timer {
  
  private long totalNanos;
  
  private long startNanos;

  
  public void reset() {
    totalNanos = startNanos = 0;
  }
  
  
  public void start() {
    totalNanos = 0;
    startNanos = System.nanoTime();
  }
  
  
  public Timer mark() {
    if (startNanos == 0)
      throw new IllegalStateException("not running");
    
    long now = System.nanoTime();
    totalNanos += (now - startNanos);
    startNanos = now;
    return this;
  }
  
  
  public long lapNanos() {
    if (startNanos == 0)
      throw new IllegalStateException("not running");
    
    return System.nanoTime() - startNanos;
  }
  
  public double lapMicros() {
    return lapNanos() * 1.0e-3;
  }
  
  public double lapMillis() {
    return lapNanos() * 1.0e-6;
  }
  
  public double lapSeconds() {
    return lapNanos() * 1.0e-9;
  }
  
  
  public Timer stop() {
    if (startNanos == 0)
      throw new IllegalStateException("not running");
    
    totalNanos += (System.nanoTime() - startNanos);
    startNanos = 0;
    
    return this;
  }
  
  
  public Timer resume() {
    if (startNanos != 0)
      throw new IllegalStateException("already running");
    startNanos = System.nanoTime();
    return this;
  }
  
  
  public long totalNanos() {
    return totalNanos;
  }
  
  public double totalMicros() {
    return totalNanos * 1.0e-3;
  }
  
  public double totalMillis() {
    return totalNanos * 1.0e-6;
  }
  
  public double totalSeconds() {
    return totalNanos * 1.0e-9;
  }

}
