/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.ipl;

/**
 * 
 */
public final class LockPolicy {
  
  public final static int DEFAULT_MAX_LOCK_PERIOD = 30 * 1000;
  public final static int MIN_MAX_LOCK_PERIOD = 100;
  
  private final int maxLockPeriod;
  
  public final static int DEFAULT_TIME_ASSERTION_FLACK = 100;
  public final static int MIN_TIME_ASSERTION_FLACK = 1;
  public final static int MAX_TIME_ASSERTION_FLACK = 1000;
  
  private final int timeAssertionFlack;
  
  
  public LockPolicy(Builder builder) {
    this.maxLockPeriod = builder.getMaxLockPeriod();
    this.timeAssertionFlack = builder.getTimeAssertionFlack();
  }
  
  

  
  
  public int getTimeAssertionFlack() {
    return timeAssertionFlack;
  }
  

  
  
  public int getMaxLockPeriod() {
    return maxLockPeriod;
  }
  
  
  
  
  
  
  
  
  
  
  
  @Override
  public String toString() {
    return "LockPolicy[ maxLockPeriod=" + maxLockPeriod + ", timeAssertionFlack=" + timeAssertionFlack + " ]";
  }











  public final static class Builder {
    
    private int maxLockPeriod = DEFAULT_MAX_LOCK_PERIOD;
    
    private int timeAssertionFlack = DEFAULT_TIME_ASSERTION_FLACK;
    
    
    
    public Builder setMaxLockPeriod(int maxPeriod) {
      if (maxPeriod < MIN_MAX_LOCK_PERIOD)
        throw new IllegalArgumentException("maxPeriod " + maxPeriod + " < " + MIN_MAX_LOCK_PERIOD);
      
      this.maxLockPeriod = maxPeriod;
      return this;
    }
    
    
    public int getMaxLockPeriod() {
      return maxLockPeriod;
    }
    
    
    
    
    
    
    
    
    
    public int getTimeAssertionFlack() {
      return timeAssertionFlack;
    }


    public void setTimeAssertionFlack(int timeAssertionFlack) {
      if (timeAssertionFlack < MIN_TIME_ASSERTION_FLACK || timeAssertionFlack > MAX_TIME_ASSERTION_FLACK)
        throw new IllegalArgumentException("out of bounds timeAssertionFlack: " + timeAssertionFlack);
      this.timeAssertionFlack = timeAssertionFlack;
    }


    public LockPolicy toPolicy() {
      return new LockPolicy(this);
    }
    
  }

}
