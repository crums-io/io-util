/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.ticker;


/**
 * A simple (and hopefully fast) tick counter with support for tick-speed.
 */
public class TickCounter extends Ticker {
  
  public final static int TIME_UNITS_PER_SECOND = 1000;
  
  private long startTime;
  private long count;
  
  private long markedEndTime;
  private long markedCount;

  /**
   * 
   */
  public TickCounter() {
    startTime = now();
  }
  
  
  /**
   * Implementation hook for time unit. If you update this to say
   * System.nanoTime(), then also update the constant TIME_UNITS_PER_SECOND.
   */
  private long now() {
    return System.currentTimeMillis();
  }
  
  /**
   * Resets the instance (as if new). The start time is set to now,
   * and the marked lap is discarded.
   * 
   * @return {@code this}
   */
  public TickCounter reset() {
    startTime = now();
    markedCount = markedEndTime = count = 0;
    return this;
  }

  /**
   * Adds one to the counter.
   */
  @Override
  public void tick() {
    ++count;
  }
  
  
  /**
   * Marks the current lap statitics. Invoked this method to seal instance's
   * current values.
   * 
   * @return {@code this}
   */
  public TickCounter mark() {
    markedEndTime = now();
    markedCount = count;
    return this;
  }
  
  
  /**
   * Discards the mark if any.
   * 
   * @return {@code this}
   */
  public TickCounter discardMark() {
    markedCount = markedEndTime = 0;
    return this;
  }
  
  
  /**
   * Returns the number of ticks since construction or since the last
   * {@linkplain #reset() reset}, if any.
   */
  public long getCount() {
    return count;
  }
  
  
  /**
   * Returns the start time. The start time is set at construction
   * or at the time {@linkplain #reset() reset} was last invoked.
   */
  public long getStartTime() {
    return startTime;
  }
  
  
  
  public long getMarkedCount() {
    return markedCount;
  }
  
  
  /**
   * Returns the average number of ticks per second. If the instance is marked,
   * the marked rate is returned; otherwise the current count 
   */
  public double getTickRate() {
    if (markedEndTime > startTime)
      return calcTickRate(count, now());
    else
      return calcTickRate(markedCount, markedEndTime);
  }
  
  private double calcTickRate(long count, long endTime) {
    long duration = Math.max( 1L, endTime - startTime );
    double fcount = count * ((double) TIME_UNITS_PER_SECOND);
    return fcount / duration;
  }
  

}
