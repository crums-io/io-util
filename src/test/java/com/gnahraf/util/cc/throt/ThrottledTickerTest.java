/*
 * Copyright 2014 Babak Farhang 
 */
package com.gnahraf.util.cc.throt;


import static org.junit.Assert.*;

import java.util.logging.Logger;
import org.junit.Test;

import com.gnahraf.test.TestHelper;
import com.gnahraf.util.cc.throt.ThrottledTicker;

/**
 * 
 * @author Babak
 */
public class ThrottledTickerTest {
  
  private final static Logger LOG = Logger.getLogger(ThrottledTickerTest.class.getName());

  @Test
  public void demo5ms() {
    String method = TestHelper.method(new Object() { });
    final long targetAvg = 5 * 1000 * 1000;
    
    ThrottledTicker ticker = new ThrottledTicker();
    ticker.setThrottleNanos(targetAvg);
    
    final int count = 100;
    LOG.info("[" + method + "] invoking " + count + " ticks");
    
    long now = System.nanoTime();
    for (int i = count; i-- > 0; )
      ticker.tick();
    long elapsedNanos = System.nanoTime() - now;

    LOG.info("[" + method + "] ticker.getThrottleNanos(): " + ticker.getThrottleNanos());
    LOG.info("[" + method + "] actual average nanos: " + elapsedNanos / count);
  }

  @Test
  public void demo50point7ms() {
    String method = TestHelper.method(new Object() { });
    final long targetAvg = 50700 * 1000;
    
    ThrottledTicker ticker = new ThrottledTicker();
    ticker.setThrottleNanos(targetAvg);
    
    final int count = 20;
    LOG.info("[" + method + "] invoking " + count + " ticks");
    
    long now = System.nanoTime();
    for (int i = count; i-- > 0; )
      ticker.tick();
    long elapsedNanos = System.nanoTime() - now;

    LOG.info("[" + method + "] ticker.getThrottleNanos(): " + ticker.getThrottleNanos());
    LOG.info("[" + method + "] actual average nanos: " + elapsedNanos / count);
  }
  
  
}
