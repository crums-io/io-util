/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.util.cc.throt;


import static org.junit.jupiter.api.Assertions.*;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import org.junit.jupiter.api.Test;


import io.crums.testing.SelfAwareTestCase;


/**
 * 
 * @author Babak
 */
public class FuzzyThrottlerTest extends SelfAwareTestCase {
  
  private final static Logger LOG = System.getLogger(FuzzyThrottlerTest.class.getName());

  @SuppressWarnings("unused")
  @Test
  public void demo() {
    
    final String method = method(new Object() { });
    
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
    LOG.log(Level.INFO, "["+ method + "]: cruiserSpeed");
    throttler.updateThrottle();
    throttler.updateThrottle();
    
    FuzzySpeed ramp0 =
        speed.setAccelerating(0.3).setCruising(0.8).setDecelerating(0.1)
        .setTooFast(0.2).setJustRight(0.8).setTooSlow(0.05).snapshot();

    LOG.log(Level.INFO, "["+ method + "]: ramp0");
    throttler.updateThrottle();
    throttler.updateThrottle();
    FuzzySpeed ramp1 =
        speed.setAccelerating(0.7).setCruising(0.3).setDecelerating(0)
        .setTooFast(0.7).setJustRight(0.2).setTooSlow(0).snapshot();
    LOG.log(Level.INFO, "["+ method + "]: ramp1");
    throttler.updateThrottle();
    throttler.updateThrottle();
    
    FuzzySpeed glide0 =
        speed.set(ramp0).setAccelerating(0.2).setCruising(0.4).setDecelerating(0.7)
        .snapshot();
    LOG.log(Level.INFO, "["+ method + "]: glide0");
    throttler.updateThrottle();
    throttler.updateThrottle();
    
    FuzzySpeed glide1 =
        speed.setAccelerating(0.2).setCruising(0.7).setDecelerating(0.3)
        .setTooFast(0.2).setJustRight(0.7).setTooSlow(0.05)
        .snapshot();
    LOG.log(Level.INFO, "["+ method + "]: glide1");

    throttler.updateThrottle();
    throttler.updateThrottle();

    LOG.log(Level.INFO, "["+ method + "]: cruiserSpeed");
    speed.set(cruiserSpeed);
    throttler.updateThrottle();
    throttler.updateThrottle();
    
  }

}
