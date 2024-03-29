/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.util.cc.throt;


import java.lang.System.Logger.Level;

import io.crums.util.cc.ThreadUtils;
import io.crums.util.ticker.Ticker;

/**
 * <p>
 * A throttled ticker.
 * </p>
 * <h2>Concurrent Access OK</h2>
 * <p>
 * This is designed to be well-behaved under concurrent access.
 * </p>
 */
public class ThrottledTicker extends Ticker {
  
  
  private volatile long throttleNanos;


  /**
   * Returns the minimum number of nanoseconds each invocation of {@linkplain #tick()}
   * takes to complete.
   */
  public long getThrottleNanos() {
    return throttleNanos;
  }


  /**
   * Sets the minimum number of nanoseconds each invocation of {@linkplain #tick()}
   * takes to complete. Setting the value to less than one effectively clears the
   * throttle.
   */
  public void setThrottleNanos(long throttleNanos) {
    this.throttleNanos = throttleNanos;
  }
  
  /**
   * Possibly throttled ticker.
   * 
   * @see #getThrottleNanos()
   */
  @Override
  public void tick() {
    if (throttleNanos < 1)
      return;
    try {
      ThreadUtils.ensureSleepNanos(throttleNanos);
    } catch (InterruptedException ix) {
      System.getLogger(ThrottledTicker.class.getName())
        .log(Level.WARNING, "Interrupted while throttling tick()");
      Thread.currentThread().interrupt();   // reset the interrupt flag
    }
  }
  
  
  

}
