/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.test;


import java.util.Random;

/**
 * A random access, random sequence. This probably doesn't work so well (in terms of
 * the random sequence's statistics), but it might be good enough for testing purposes.
 * The idea is to be able to access the <em>n</em><sup><small>th</small></sup> value in
 * the sequence without having to iterate through the <em>n - 1</em> values before it.
 * <p/>
 * <h3>Thread Safety</h3>
 * <p/>
 * <em>Not safe for concurrent access!</em>
 * 
 * @author Babak
 */
public class RandomSequence {
  
  private final long initSeed;
  private final int seedRate;
  
  private final Random generator;
  
  
  private long index;
  
  /**
   * @param seed
   *        the starting seed
   * @param seedRate
   *        the rate at which the seed is reset. Every <tt>sampleRate</tt> invocations of
   *        {@linkplain #next()} resets the generator's seed. Designed for use
   *        in hundreds; must be at least 2.
   */
  public RandomSequence(long seed, int seedRate) {
    if (seedRate < 2)
      throw new IllegalArgumentException("required seedRate >= 2; actual was " + seedRate);
    this.initSeed = seed;
    this.generator = new Random(seed);
    this.seedRate = seedRate;
  }
  
  
  private long nthSeed(long n) {
    return initSeed + n;
  }
  
  
  private long seedForIndex(long index) {
    return nthSeed(index / seedRate);
  }
  
  
  public final long initSeed() {
    return initSeed;
  }
  
  
  public long next() {
    long next = generator.nextLong();

    if (++index % seedRate == 0) {
      long n = index / seedRate;
      generator.setSeed(nthSeed(n));
    }
    return next;
  }
  
  /**
   * Returns the index associated with next invocation of {@linkplain #next()}. (The index of
   * the previous invocation of <tt>next()</tt> is one less than this.)
   * 
   * @see #jumpTo(long)
   */
  public final long index() {
    return index;
  }
  
  /**
   * The rate at which the underlying random number generator's seed is reset.
   * @return
   */
  public final int seedRate() {
    return seedRate;
  }
  
  
  
  /**
   * Jumps to the given <tt>index</tt>. On return, {@linkplain #index()} returns supplied argument.
   * With few exceptions, jumping to a multiple of the {@linkplain #seedRate() seed rate} is more
   * efficient.
   */
  public RandomSequence jumpTo(long index) {
    
    if (index == this.index)
      return this;
    
    if (index < 0)
      throw new IllegalArgumentException("index: " + index);

    long seed = seedForIndex(index);
    boolean useGeneratorAsIs = index > this.index && seed == seedForIndex(this.index);
    if (!useGeneratorAsIs) {
      generator.setSeed(seed);
      this.index = index - (index % seedRate);
    }
    
    while (index > index())
      next();
    
    return this;
  }

}
