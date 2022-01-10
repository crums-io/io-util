/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.util.cc.throt;

/**
 * 
 * @author Babak
 */
public class FuzzySpeedBean extends FuzzySpeed {
  
  private double accelerating, cruising, decelerating, tooFast, justRight, tooSlow;

  

  @Override
  public double accelerating() {
    return accelerating;
  }

  @Override
  public double cruising() {
    return cruising;
  }

  @Override
  public double decelerating() {
    return decelerating;
  }

  @Override
  public double tooFast() {
    return tooFast;
  }

  @Override
  public double justRight() {
    return justRight;
  }

  @Override
  public double tooSlow() {
    return tooSlow;
  }
  
  

  public FuzzySpeedBean setAccelerating(double accelerating) {
    this.accelerating = accelerating;
    return this;
  }

  public FuzzySpeedBean setCruising(double cruising) {
    this.cruising = cruising;
    return this;
  }

  public FuzzySpeedBean setDecelerating(double decelerating) {
    this.decelerating = decelerating;
    return this;
  }

  public FuzzySpeedBean setTooFast(double tooFast) {
    this.tooFast = tooFast;
    return this;
  }

  public FuzzySpeedBean setJustRight(double justRight) {
    this.justRight = justRight;
    return this;
  }

  public FuzzySpeedBean setTooSlow(double tooSlow) {
    this.tooSlow = tooSlow;
    return this;
  }
  
  
  public synchronized FuzzySpeedBean set(FuzzySpeed copy) {
    setAccelerating(copy.accelerating());
    setCruising(copy.cruising());
    setDecelerating(copy.decelerating());
    setTooFast(copy.tooFast());
    setJustRight(copy.justRight());
    setTooSlow(copy.tooSlow());
    return this;
  }

}
