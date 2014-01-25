/*
 * Copyright 2014 Babak Farhang 
 */
package com.gnahraf.util.ticker;

import java.util.Collection;
import java.util.Iterator;

/**
 * 
 * 
 * @author Babak
 */
public class TickerPack extends Ticker {
  
  private final Ticker[] stack;
  
  public TickerPack(Collection<Ticker> tickers) {
    this.stack = new Ticker[tickers.size()];
    // copy the tickers (in reverse--potentially faster?)
    Iterator<Ticker> iter = tickers.iterator();
    for (int index = stack.length; index-- > 0; )
      stack[index] = iter.next();
  }

  @Override
  public void tick() {
    for (int index = stack.length; index-- > 0; )
      stack[index].tick();
  }
  
  

}
