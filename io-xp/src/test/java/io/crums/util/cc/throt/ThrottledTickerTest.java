/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.util.cc.throt;


import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import org.junit.jupiter.api.Test;

import com.gnahraf.test.SelfAwareTestCase;

/**
 * 
 * @author Babak
 */
public class ThrottledTickerTest extends SelfAwareTestCase {
  
  private final static Logger LOG = System.getLogger(ThrottledTickerTest.class.getName());

  @Test
  public void demo5ms() {
    String method = method(new Object() { });
    final long targetAvg = 5 * 1000 * 1000;
    
    ThrottledTicker ticker = new ThrottledTicker();
    ticker.setThrottleNanos(targetAvg);
    
    final int count = 100;
    LOG.log(Level.INFO, "[" + method + "] invoking " + count + " ticks");
    
    long now = System.nanoTime();
    for (int i = count; i-- > 0; )
      ticker.tick();
    long elapsedNanos = System.nanoTime() - now;

    LOG.log(Level.INFO, "[" + method + "] ticker.getThrottleNanos(): " + ticker.getThrottleNanos());
    LOG.log(Level.INFO, "[" + method + "] actual average nanos: " + elapsedNanos / count);
  }

  @Test
  public void demo50point7ms() {
    String method = method(new Object() { });
    final long targetAvg = 50700 * 1000;
    
    ThrottledTicker ticker = new ThrottledTicker();
    ticker.setThrottleNanos(targetAvg);
    
    final int count = 20;
    LOG.log(Level.INFO, "[" + method + "] invoking " + count + " ticks");
    
    long now = System.nanoTime();
    for (int i = count; i-- > 0; )
      ticker.tick();
    long elapsedNanos = System.nanoTime() - now;

    LOG.log(Level.INFO, "[" + method + "] ticker.getThrottleNanos(): " + ticker.getThrottleNanos());
    LOG.log(Level.INFO, "[" + method + "] actual average nanos: " + elapsedNanos / count);
  }
  
  
}
