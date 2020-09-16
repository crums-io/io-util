/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.math.stats;

/**
 * Gathers simple statistics.
 * 
 * @author Babak
 */
public class SimpleSampler extends Sampler {
  
  private int count;
  private double total;
  private double squareTotal;
  
  private double min = Double.MAX_VALUE;
  private double max = Double.MIN_VALUE;
  
  
  
  
  public SimpleSampler() {  }
  
  
  
  public SimpleSampler(SimpleSampler copy) {
    this.count = copy.count;
    this.total = copy.total;
    this.squareTotal = copy.squareTotal;
    this.min = copy.min;
    this.max = copy.max;
  }

  @Override
  public void observe(double value) {
    if (min > value)
      min = value;
    if (max < value)
      max = value;
    
    total += value;
    squareTotal += value * value;
    ++count;
  }

  
  @Override
  public void clear() {
    min = Double.MAX_VALUE;
    max = Double.MIN_VALUE;
    total = squareTotal = count = 0;
  }

  public int getCount() {
    return count;
  }
  
  public double sum() {
    return total;
  }

  public double getMean() {
    return total / count;
  }

  public double getMeanSquare() {
    return squareTotal /count;
  }
  
  public double getSd() {
    double mean = getMean();
    double meanSq = getMeanSquare();
    return Math.sqrt(meanSq - mean * mean);
  }

  public double getMin() {
    return min;
  }

  public double getMax() {
    return max;
  }

}
