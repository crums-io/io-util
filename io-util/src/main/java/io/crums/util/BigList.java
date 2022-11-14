/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;

import java.util.Objects;
import java.util.function.Function;

/**
 * Yet another attempt retrofit large abstract lists into the Java
 * 4-byte model. Such lists are not designed to fit in memory: they're
 * just a useful interface abstraction for things that can be random
 * accessed by index. The usual problem is that the indexes are bounded
 * by 31-bit values.
 * <p>
 * I'm hopeful I can easily add search capabilities for cases when the
 * abstract list is sorted.
 * </p>
 */
public class BigList<T> extends Lists.RandomAccessList<T> {
  
  
  private final Function<Long, T> getFunc;
  private final long baseIndex;
  private final long longSize;
  private final int size;
  
  
  /**
   * Creates an instance with {@code baseIndex} 0, and the maximum size
   * possible.
   * 
   * @param getFunc   by-index lookup function
   * @param longSize  the actual size of the abstract list
   */
  public BigList(Function<Long, T> getFunc, long longSize) {
    this(getFunc, 0, longSize, (int) Math.min(Integer.MAX_VALUE, longSize));
  }
  
  /**
   * Full constructor.
   * 
   * @param getFunc   by-index lookup function
   * @param baseIndex the base index (0 &le; {@code baseIndex} &le; {@code longSize})
   * @param longSize  the actual size of the abstract list
   * @param size      size of the view (0 &le; {@code size} &le; {@code longSize} - {@code baseIndex})
   */
  public BigList(Function<Long, T> getFunc, long baseIndex, long longSize, int size) {
    this.getFunc = Objects.requireNonNull(getFunc, "null get-function");
    this.baseIndex = baseIndex;
    this.longSize = longSize;
    this.size = size;
    
    if (baseIndex < 0)
      throw new IllegalArgumentException("negative baseIndex: " + baseIndex);
    if (longSize < baseIndex)
      throw new IllegalArgumentException("base-index " + baseIndex + " > long-size " + longSize);
    if (size < 0)
      throw new IllegalArgumentException("negative size: " + size);
    if (size > longSize - baseIndex)
      throw new IllegalArgumentException(
          "size " + size + " > long-size (" + longSize + ") - base-index (" + baseIndex + ")");
  }

  @Override
  public T get(int index) {
    Objects.checkIndex(index, size);
    return getFunc.apply(baseIndex + index);
  }

  /**
   * @see #longSize()
   */
  @Override
  public int size() {
    return size;
  }
  
  /**
   * Returns the base index. Indexed access (i.e. {@linkplain #get(int)}) is relative
   * to this base index.
   */
  public long baseIndex() {
    return baseIndex;
  }
  
  /** Returns the actual size of the backing abstract list. May be greater than {@linkplain #size()}. */
  public long longSize() {
    return longSize;
  }
  
  /**
   * Returns a view of the backing abstract list starting from the given base index and with
   * the given number of elements.
   * 
   * @param baseIndex the base index (0 &le; {@code baseIndex} &le; {@code longSize()})
   * @param size      size of the view (0 &le; {@code size} &le; {@code longSize()} - {@code baseIndex})
   */
  public BigList<T> slice(long baseIndex, int size) {
    return new BigList<>(getFunc, baseIndex, longSize, size);
  }

}




