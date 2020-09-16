/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.util.ticker;

import io.crums.math.stats.MovingAverage;



/**
 * A ticker that maintains a moving average of the lap times between invocations
 * of {@linkplain #tick()}.
 * <p>
 * <em>Not thread safe!</em>
 * </p>
 * 
 * @author Babak
 */
public class MovingAvgTicker extends Ticker {
  
  
  private final MovingAverage window;
  private long lapNanos;
  
  
  

  
  public MovingAvgTicker(int windowSize, long initLapMillis) {
    this.window = new MovingAverage(windowSize, initLapMillis * 1000 * 1000);
    lapNanos = System.nanoTime();
  }
  
  
  public void clear(long initLapNanos) {
    window.clear(initLapNanos);
    lapNanos = System.nanoTime();
  }
  
  
  /**
   * Returns the moving average in nanoseconds.
   */
  public long movingLapAvg() {
    return window.average();
  }
  
  
  public int windowSize() {
    return window.windowSize();
  }
  
  
  public long tickCount() {
    return window.count();
  }
  
  
  @Override
  public void tick() {
    long now = System.nanoTime();
    window.observe(now - lapNanos);
    lapNanos = now;
  }

}
