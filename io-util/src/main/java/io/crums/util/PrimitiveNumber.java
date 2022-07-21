/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;

import java.util.Objects;


/**
 * Generalization of boxed number primitives.
 * 
 * <h3>Motivation</h3>
 * <p>
 * The primitive boxed types ({@code Byte}, {@code Short}, {@code Integer}, {@code Long},
 * {@code Float}, and {@code Double}) are not mutually comparable (altho, <em>when unboxed</em>,
 * operators like {@code ==}, {@code >}, and {@code !=} <em>do work</em>). The aim here is
 * to implement the equality and comparsion semantics for mixed primitives types.
 * </p>
 * <h4>Use Case</h4>
 * <p>
 * When dealing with numbers from "external" systems and comparing them with their internal
 * representation, it would be useful to be able to compare them even if they're not exactly
 * the same type. The specific use case I'm running into is with JSON parsing. Some of my
 * objects have both a {@code long} and {@code int} member, but a JSON parser may read both as
 * {@code int}s. Usually this presents no problem, but if you're using these values in objects
 * encapsulating adhoc predicates (e.g. are these 2 numbers equal?) then you'll need to use
 * some previously agreed, hopefully reasonable conversion rules. Hence this class.
 * </p>
 * 
 * @deprecated Ditching this: finding ways not to use it.
 */
@SuppressWarnings("serial")
public abstract class PrimitiveNumber extends Number implements Comparable<PrimitiveNumber> {

  
  
  PrimitiveNumber(Number value) {
    Objects.requireNonNull(value, "null value");
  }
  
  /** Returns the boxed primitive wrapped by this instance. */
  public abstract Number getPrimitive();
  
  
  /**
   * Sets this instance's value. <em>Optional operation.</em>
   * 
   * @param value a boxed primitive
   * @return the previous boxed primitive
   * 
   * @throws UnsupportedOperationException 
   */
  protected Number setValueImpl(Number value) throws UnsupportedOperationException {
    
    throw new UnsupportedOperationException();
  }
  
  
  public Number setValue(Long value) throws UnsupportedOperationException {
    return setValueImpl(value);
  }
  
  
  public Number setValue(Integer value) throws UnsupportedOperationException {
    return setValueImpl(value);
  }
  
  
  public Number setValue(Double value) throws UnsupportedOperationException {
    return setValueImpl(value);
  }
  
  
  public Number setValue(Float value) throws UnsupportedOperationException {
    return setValueImpl(value);
  }
  
  
  /**
   * Sets the value of this number. Only boxed primitives are allowed.
   * 
   * @throws UnsupportedOperationException if {@code value} is not a boxed primitive
   */
  public Number setValue(Number value) throws UnsupportedOperationException {
    boolean ok =
        value instanceof Long ||
        value instanceof Double ||
        value instanceof Integer ||
        value instanceof Float ||
        value instanceof Short ||
        value instanceof Byte;
    if (!ok)
      throw new IllegalArgumentException("unsupported number type: " + value.getClass());
    return setValueImpl(value);
  }
  
  
  
  
  
  /**
   * Determines whether this instance is mutable.
   * 
   * @see #setValue(Number)
   * @see #isReadOnly()
   */
  public abstract boolean isSettable();
  
  /**
   * Determines whether this instance is immutable.
   * 
   * @return {@code !isSettable()}
   */
  public final boolean isReadOnly() {
    return !isSettable();
  }


  @Override
  public final int intValue() {
    return getPrimitive().intValue();
  }

  @Override
  public final long longValue() {
    return getPrimitive().longValue();
  }

  @Override
  public final float floatValue() {
    return getPrimitive().floatValue();
  }

  @Override
  public final double doubleValue() {
    return getPrimitive().doubleValue();
  }
  
  
  public final int compareTo(PrimitiveNumber other) {
    return PrimitiveComparator.compareNumbers(this, other);
  }
  
  /**
   * <p>
   * Uses {@linkplain PrimitiveComparator#equal(Number, Number)} and
   * {@linkplain PrimitiveComparator#hashCode(Number)}.
   * </p>
   * {@inheritDoc}
   */
  @Override
  public final boolean equals(Object o) {
    return o == this ||
        o instanceof PrimitiveNumber other &&
        PrimitiveComparator.equal(getPrimitive(), other.getPrimitive()) &&
        hashCode() == other.hashCode();
  }
  

  /** See {@linkplain PrimitiveComparator#hashCode(Number)}. */
  @Override
  public final int hashCode() {
    return PrimitiveComparator.hashCode(getPrimitive());
  }
  
  
  /** Immutable type. */
  public static class Fixed extends PrimitiveNumber {
    
    private final Number value;
    
    public Fixed(Long value) {
      this(value, true);
    }
    
    public Fixed(Integer value) {
      this(value, true);
    }
    
    public Fixed(Short value) {
      this(value, true);
    }
    
    public Fixed(Double value) {
      this(value, true);
    }
    
    public Fixed(Float value) {
      this(value, true);
    }
    
    private Fixed(Number value, boolean ignore) {
      super(value);
      this.value = value;
    }

    @Override
    public final Number getPrimitive() {
      return value;
    }

    /** @return {@code false} */
    @Override
    public final boolean isSettable() {
      return false;
    }
    
  }
  
  
  
  public static boolean isSettable(Number number) {
    return number instanceof Settable;
  }
  
  
  
  /**
   * Mutable type.
   * 
   * @see #setValue(Number)
   */
  public static class Settable extends PrimitiveNumber {
    
    private Number value;
    
    
    public Settable() {
      this(0L);
    }
    
    public Settable(Long value) {
      this(value, true);
    }
    
    public Settable(Integer value) {
      this(value, true);
    }
    
    public Settable(Short value) {
      this(value, true);
    }
    
    public Settable(Double value) {
      this(value, true);
    }
    
    public Settable(Float value) {
      this(value, true);
    }
    
    private Settable(Number value, boolean ignore) {
      super(value);
      this.value = value;
    }
    
    
    
    
    

    @Override
    public final Number getPrimitive() {
      return value;
    }

    @Override
    public boolean isSettable() {
      return true;
    }

    @Override
    protected Number setValueImpl(Number value) throws UnsupportedOperationException {
      var oldValue = this.value;
      this.value = value;
      return oldValue;
    }
    
  }

}
