/*
 * Copyright 2014 Babak Farhang 
 */
package com.gnahraf.util.cc.throt;


import java.text.DecimalFormat;

import org.apache.log4j.Logger;

import com.gnahraf.util.ticker.Ticker;

/**
 * An experimental fuzzy controller for throttling.
 * 
 * @see FuzzySpeed
 * @author Babak
 */
public class FuzzyThrottler {
  
  private final static Logger LOG = Logger.getLogger(FuzzyThrottler.class);
  
  private final ThrottledTicker ticker = new ThrottledTicker();
  /**
   * This <tt>speed</tt> object represents a live, qualitative measurement of
   * the activity that must be throttled.
   */
  protected final FuzzySpeed speed;
  private final double w;
  
//  private final 
  
  /**
   * Creates a new instance with the given <em>speed</em> implementation.
   * 
   * @see #speed
   */
  public FuzzyThrottler(FuzzySpeed speed) {
    if (speed == null)
      throw new IllegalArgumentException("null fuzzy speed");
    
    this.speed = speed;
    
    w = Math.sqrt(
        sq(wfa()) +
        sq(wfc()) +
        sq(wfd()) +
        sq(wja()) +
        sq(wjd()) +
        sq(wsa()) +
        sq(wsc()) +
        sq(wsd()));
  }
  
  private double sq(double w) {
    return w * w;
  }
  
  
  
  
  /**
   * Updates the throttle based on the current state of the {@linkplain #speed}.
   * This method should be invoked once in a while (but not too often!). The most
   * natural time to invoke it, is when the state of instance's speed has changed.
   */
  public void updateThrottle() {
    long currentThrottleNanos = ticker.getThrottleNanos();
    
    // take a snapshot of the speed so there are no races as the speed changes
    FuzzySpeed snapshot = speed.snapshot();
    
    final double a = snapshot.accelerating();
    final double c = snapshot.cruising();
    final double d = snapshot.decelerating();
    
    final double f = snapshot.tooFast();
    final double j = snapshot.justRight();
    final double s = snapshot.tooSlow();
    
    double tfa = a * f * wfa();
    double tfc = c * f * wfc(); 
    double tfd = d * f * wfd();
    
    double tja = a * j * wja();
    // no tjc: wjc() is by definition zero
    double tjd = d * j * wjd();
    
    double tsa = a * s * wsa();
    double tsc = c * s * wsc();
    double tsd = d * s * wsd();
    
    double t = (tfa + tfc + tfd + tja + tjd + tsa + tsc + tsd) / w;
    
    // bounded in the range 50 micros to 15 sec..
    long newThrottleNanos = (long) Math.min( Math.max(5e4, (1 + t) * currentThrottleNanos), 15e9);
    
    LOG.info("Throttling to " + new DecimalFormat("#,###").format(newThrottleNanos) + " nanoseconds");
    ticker.setThrottleNanos(newThrottleNanos);
  }
  
  

  private double wfa() {
    return 0.5;
  }
  
  private double wfc() {
    return 0.1;
  }
  
  private double wfd() {
    return 0;
  }
  

  
  private double wja() {
    return 0.1;
  }

  private double wjd() {
    return -0.1;
  }
  

  private double wsa() {
    return 0;
  }

  private double wsc() {
    return -0.1;
  }

  private double wsd() {
    return -0.5;
  }
  
  

  /**
   * Returns the ticker this instance throttles. The process that must be throttled
   * must somehow periodically invoke the returned ticker's {@linkplain Ticker#tick()
   * tick()} method in for it to be throttled by this instance.
   */
  public Ticker throttledTicker() {
    return ticker;
  }

}
