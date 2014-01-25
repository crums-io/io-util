/*
 * Copyright 2014 Babak Farhang 
 */
package com.gnahraf.util.cc.throt;

/**
 * Fuzzy membership set abstraction for <em>speed state</em>. The return value of each member function (method)
 * represents the degree by which the speed is in that state. Valid return values for any method are
 * anything non-negative, although typically normalized return values in the range <tt>[0, 1]</tt> will be used.
 * <p/>
 * Think of <em>speed</em> here as some metric that when too high must be throttled.
 * It needn't be time-based;
 * it may be resource-based: for example, the number of threads servicing requests. To implement a method,
 * think of how a human monitoring an analog dial of the
 * metric might interpret the adjective represented by the method name (for example <em>accelerating</em>)
 * and return 0 if inapt, 1 if certainly apt, or any where in between for the gray areas. It really doesn't
 * matter if you only return one of say 3 or 4 values: it works surprisingly well with simple, crude
 * models. 
 * 
 * @see #snapshot()
 * @author Babak
 */
public abstract class FuzzySpeed {
  
  /**
   * Is the needle rising? Second order property.
   */
  public abstract double accelerating();
  /**
   * Is the needle holding steady? Second order property. 
   */
  public abstract double cruising();
  /**
   * Is the needle descending? Second order property.
   */
  public abstract double decelerating();
  
  /**
   * Are we going too fast? (Is the needle in the the red zone on the dial?)
   */
  public abstract double tooFast();
  /**
   * Are we chugging along just fine?
   */
  public abstract double justRight();
  /**
   * Are we going too slow? (The needle ought to be pointing higher on the dial.)
   */
  public abstract double tooSlow();
  
  
  /**
   * Returns a fixed snapshot of the instance's state.
   * <p/>
   * Note this method synchronizes on the instance itself. The goal here is
   * that a subclass only ever mutate an instance's state while holding the
   * instance's monitor. That way, this method can truly guarantee a <em>snapshot</em>
   * of the speed state.
   */
  public synchronized FuzzySpeed snapshot() {
    
    final double a = accelerating();
    final double c = cruising();
    final double d = decelerating();
    final double f = tooFast();
    final double j = justRight();
    final double s = tooSlow();
    
    return new FuzzySpeed() {
//      @Override
//      public double wayTooFast() {
//        return w;
//      }
      @Override
      public double tooSlow() {
        return s;
      }
      @Override
      public double tooFast() {
        return f;
      }
      @Override
      public double justRight() {
        return j;
      }
      @Override
      public double decelerating() {
        return d;
      }
      @Override
      public double cruising() {
        return c;
      }
      @Override
      public double accelerating() {
        return a;
      }
    };
  }

}
