/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.util;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A lazily-evaluated value. Basically, this takes an ordinary supplier
 * and makes it idempotent by caching the result of its first {@code get()}.
 * 
 * @see #get()
 * @see #of(Supplier)
 */
public class SuppliedValue<T> implements Supplier<T> {
  
  /**
   * Pseudo constructor returns an instance of this class. This method
   * checks for instances of this class, so that the argument is not needlessly
   * wrapped.
   * 
   * @param supplier the underlying supplier that will be called at most once
   * 
   * @return a new instance, if {@code supplier} is not an instance of this class;
   *         {@code supplier} argument, otherwise
   */
  @SuppressWarnings("unchecked")
  public final static <T> SuppliedValue<T> of(Supplier<T> supplier) {
    return supplier instanceof SuppliedValue sv ?
        sv :
          new SuppliedValue<>(supplier);
  }
  
  private final Supplier<T> base;
  private T value;

  
  /**
   * Creates a new instance using another supplier.
   * @see #of(Supplier)
   */
  public SuppliedValue(Supplier<T> base) throws NullPointerException {
    this.base = Objects.requireNonNull(base);
  }

  /**
   * Returns the <em>same</em> object across invocations.
   */
  @Override
  public final T get() {
    if (value == null)
      value = base.get();
    return value;
  }
  
  
  public Optional<T> peek() {
    return value == null ? Optional.empty() : Optional.of(value);
  }

}



