/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.ticker;

import java.util.Objects;

/**
 * Flaps a relay-ticker every so many ticks.
 * 
 * <h2>Single Thread Use</h2>
 * <p><em>This class is not threadsafe!</em></p>
 */
public class TickFlapper extends Ticker {
  
  private final Ticker relay;
  
  private int tickPeriod;
  
  private int countdown;
  
  

  /**
   * 
   * @param relay the ticker to which events are relayed
   * @param tickPeriod every this-many ticks, a tick is relayed (&ge; 1)
   */
  public TickFlapper(Ticker relay, int tickPeriod) {
    this.relay = Objects.requireNonNull(relay, "null relay ticker");
    setTickPeriod(tickPeriod);
    this.countdown = tickPeriod;
  }

  /**
   * Counts down 1 on every tick. If the {@linkplain #getCountdown() count-down} is zero
   * (or less if, against advice, is invoked concurrently), then the tick
   * is passed down to the relay (set at construction {@linkplain #TickFlapper(Ticker, int)})
   * and the count-down is reset.
   */
  @Override
  public void tick() {
    if (--countdown <= 0) {
      countdown = tickPeriod;
      relay.tick();
    }

  }

  /**
   * Returns the tick period. Every this-many ticks, a tick is relayed.
   * 
   * @return &ge; 1
   * 
   * @see #setTickPeriod(int)
   */
  public final int getTickPeriod() {
    return tickPeriod;
  }

  /**
   * Returns the current value of the periodic count-down, from the
   * {@linkplain #getTickPeriod() tick period} down to zero.
   */
  public final int getCountdown() {
    return countdown;
  }

  /**
   * Sets the periodicity of relayed ticks. This takes effect at the
   * start of the next count-down.
   * 
   * @param tickPeriod every this-many ticks, a tick is relayed (&ge; 1)
   * 
   * @see #getTickPeriod()
   */
  public final void setTickPeriod(int tickPeriod) {
    if (tickPeriod < 1)
      throw new IllegalArgumentException("tickPeriod " + tickPeriod);
    
    this.tickPeriod = tickPeriod;
  }

}




