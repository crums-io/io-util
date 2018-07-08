/*
 * Copyright 2014 Babak Farhang 
 */
package com.gnahraf.util.cc.throt;


import static org.junit.Assert.*;

import java.util.logging.Logger;
import org.junit.Test;

import com.gnahraf.test.TestHelper;


/**
 * 
 * @author Babak
 */
public class FuzzyThrottlerTest {
  
  private final static Logger LOG = Logger.getLogger(FuzzyThrottlerTest.class.getName());

  @Test
  public void demo() {
    
    final String method = TestHelper.method(new Object() { });
    
    final FuzzySpeedBean speed = new FuzzySpeedBean();
    speed.setAccelerating(0.1);
    speed.setCruising(0.9);
    speed.setDecelerating(0.2);
    speed.setTooFast(0.1);
    speed.setJustRight(0.8);
    speed.setTooSlow(0.1);
    
    FuzzySpeed cruiserSpeed = speed.snapshot();
    
    FuzzyThrottler throttler = new FuzzyThrottler(speed);
    
    ThrottledTicker ticker = (ThrottledTicker) throttler.throttledTicker();
    assertEquals(0, ticker.getThrottleNanos());
    LOG.info("["+ method + "]: cruiserSpeed");
    throttler.updateThrottle();
    throttler.updateThrottle();
    
    FuzzySpeed ramp0 =
        speed.setAccelerating(0.3).setCruising(0.8).setDecelerating(0.1)
        .setTooFast(0.2).setJustRight(0.8).setTooSlow(0.05).snapshot();

    LOG.info("["+ method + "]: ramp0");
    throttler.updateThrottle();
    throttler.updateThrottle();
    FuzzySpeed ramp1 =
        speed.setAccelerating(0.7).setCruising(0.3).setDecelerating(0)
        .setTooFast(0.7).setJustRight(0.2).setTooSlow(0).snapshot();
    LOG.info("["+ method + "]: ramp1");
    throttler.updateThrottle();
    throttler.updateThrottle();
    
    FuzzySpeed glide0 =
        speed.set(ramp0).setAccelerating(0.2).setCruising(0.4).setDecelerating(0.7)
        .snapshot();
    LOG.info("["+ method + "]: glide0");
    throttler.updateThrottle();
    throttler.updateThrottle();
    
    FuzzySpeed glide1 =
        speed.setAccelerating(0.2).setCruising(0.7).setDecelerating(0.3)
        .setTooFast(0.2).setJustRight(0.7).setTooSlow(0.05)
        .snapshot();
    LOG.info("["+ method + "]: glide1");

    throttler.updateThrottle();
    throttler.updateThrottle();

    LOG.info("["+ method + "]: cruiserSpeed");
    speed.set(cruiserSpeed);
    throttler.updateThrottle();
    throttler.updateThrottle();
    
  }

}
