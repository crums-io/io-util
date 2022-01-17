/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.util.cc;

/**
 * Thread utilities--so far, just stuff for sleeping.
 * 
 * @author Babak
 */
public class ThreadUtils {
  
  private ThreadUtils() { }
  
  /**
   * Ensures the current thread sleeps at least the given milliseconds before
   * returning from this method.
   */
  public static void ensureSleepMillis(long millis) throws InterruptedException {
    if (millis < 1)
      return;
    long now = System.currentTimeMillis();
    final long targetTime = now + millis;
    
    do {
      Thread.sleep(millis);
      millis = targetTime - System.currentTimeMillis();
    } while (millis > 0);
  }
  
  /**
   * Ensures the current thread sleeps at least the given milliseconds before
   * returning from this method.
   */
  public static void ensureSleepNanos(long nanos) throws InterruptedException {
    if (nanos < 1)
      return;
    long now = System.nanoTime();
    final long targetTime = now + nanos;
    
    do {
      long millis = nanos / (1000*1000);
      int fractionalNanos = (int) (nanos % (1000*1000));
      Thread.sleep(millis, fractionalNanos);
      nanos = targetTime - System.nanoTime();
    } while (nanos > 0);
  }

}
