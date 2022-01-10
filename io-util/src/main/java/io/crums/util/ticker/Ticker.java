/*
 * Copyright 2014-2021 Babak Farhang 
 */
package io.crums.util.ticker;

/**
 * A ticker with mild side effects. A functional anti-pattern, obviously. Its use here
 * is for instrumentation and throttling.
 * 
 * <h2>Implementation Models</h2>
 * <p>
 * Implementations are typically designed for single thread access. This should be
 * understood as the rule, unless specifically documented otherwise.
 * </p>
 * 
 * @see #NOOP
 * @see #tick()
 */
public abstract class Ticker {
  
  /**
   * No-op, stateless, singleton. <em>Safe for concurrent access.</em>
   */
  public final static Ticker NOOP =
      new Ticker() {
        @Override public void tick() {  }
      };
  
  /**
   * A tick is noted. The general implementation contract is that, unless specifically
   * documented otherwise, this method should return quickly (on average). The <em>
   * on-average</em> proviso here means a this method may occasionally be (relatively)
   * time consuming.
   */
  public abstract void tick();

}
