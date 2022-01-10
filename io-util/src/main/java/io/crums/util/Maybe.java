/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


/**
 * Semantic sugar for ternary states and outcomes.
 * 
 * @see #code()
 */
public enum Maybe {
  
  
  NO,
  MAY_BE,
  YES;
  
  
  /**
   * Is this a {@linkplain #MAY_BE}?
   */
  public boolean maybe() {
    return this == MAY_BE;
  }

  /**
   * Is this a {@linkplain #YES}?
   */
  public boolean yes() {
    return this == YES;
  }
  
  /**
   * Is this a {@linkplain #NO}?
   */
  public boolean no() {
    return this == NO;
  }
  
  
  /**
   * Is this possible? Returns !{@linkplain #no()}}.
   * 
   * @return <tt>true</tt> if this is a {@linkplain #YES} or {@linkplain #MAY_BE}
   * 
   * @see #impossible()
   */
  public boolean possible() {
    return this != NO;
  }
  
  
  /**
   * Synonym for {@linkplain #no()}.
   * 
   * @see #possible()
   */
  public boolean impossible() {
    return no();
  }
  
  
  /**
   * Returns -1 if this is a {@linkplain #NO}, 0 if {@linkplain #MAY_BE}, and 1 if
   * {@linkplain #YES}.
   */
  public int code() {
    switch (this) {
    case NO:      return -1;
    case MAY_BE:  return 0;
    case YES:     return 1;
    }
    throw new RuntimeException("unaccounted enum " + this);
  }

}
