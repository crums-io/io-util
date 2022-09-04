/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.ticker;

import java.util.Objects;

/**
 * Utility for ticking progress.
 */
public class Progress {
  
  public final static String DEFAULT_STD_OUT_SYMBOL = " .";
  public final static int DEFAULT_RELAYED_TICKS = 20;
  
  
  /**
   * Returns an instance that ticks to std out about 20 times till completion.
   * 
   * @param expectedTicks expected number of ticks to completion
   */
  public static Ticker newStdOut(long expectedTicks) {
    return newStdOut(DEFAULT_STD_OUT_SYMBOL, DEFAULT_RELAYED_TICKS, expectedTicks);
  }

  /**
   * Returns an instance that ticks to std out the specified number of ticks till completion.
   * 
   * @param tickSymbol the symbol printed to std out per related progress tick
   * @param relayedTicks the number of {@code tickSymbol}s to print to completion
   * @param expectedTicks expected number of actual ticks to completion
   */
  public static Ticker newStdOut(String tickSymbol, int relayedTicks, long expectedTicks) {
    if (Objects.requireNonNull(tickSymbol, "null tickSymbol").isEmpty())
      throw new IllegalArgumentException("empty tickSymbol");
    Ticker display = new Ticker() {
      @Override
      public void tick() {
        System.out.print(tickSymbol);
      }
    };
    return newInstance(display, relayedTicks, expectedTicks);
  }
  
  public static Ticker newInstance(Ticker relay, int relayedTicks, long expectedTicks) {
    if (relayedTicks <= 0)
      throw new IllegalArgumentException("relayedTicks: " + relayedTicks);
    if (expectedTicks < 0)
      throw new IllegalArgumentException("expectedTicks: " + expectedTicks);
    if (relayedTicks >= expectedTicks)
      return relay;
    long tickPeriod = (expectedTicks / relayedTicks);
    if (tickPeriod > Integer.MAX_VALUE)
      throw new IllegalArgumentException(
          "overflow: expectedTicks / relayedTick > 4-byte int: (" +
          expectedTicks + "/" + relayedTicks + ")");
    return new TickFlapper(relay, (int) tickPeriod);
  }

}
