/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;

import java.nio.Buffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.AbstractList;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.RandomAccess;


/**
 * A growable list of numerical primitives using typed heap buffers.
 */
public abstract class NumberList<N extends Number> extends AbstractList<N> implements RandomAccess {
  
  
  /**
   * Creates and returns a new int list.
   * 
   * @param entriesPerBlock  max block size
   * @param initEntryCap     initial capacity
   */
  public static IntList newIntList(int entriesPerBlock, int initEntryCap) {
    return new IntList(entriesPerBlock, initEntryCap);
  }
  
  

  /**
   * Creates and returns a new long list.
   * 
   * @param entriesPerBlock  max block size
   * @param initEntryCap     initial capacity
   */
  public static LongList newLongList(int entriesPerBlock, int initEntryCap) {
    return new LongList(entriesPerBlock, initEntryCap);
  }
  
  
  
  
  
  
  protected final int entriesPerBlock;
  
  Buffer[] blocks = { };
  
  
  
  protected NumberList(int entriesPerBlock, int initEntryCap) {
    if (entriesPerBlock < 1)
      throw new IllegalArgumentException("entriesPerBlock: " + entriesPerBlock);
    this.entriesPerBlock = entriesPerBlock;
    
    // boiler plate check
    if (initEntryCap < 1)
      throw new IllegalArgumentException("initEntryCap not positive: " + initEntryCap);
    if (initEntryCap > entriesPerBlock)
      throw new IllegalArgumentException(
          "initEntryCap " + initEntryCap + " > entriesPerBlock" + entriesPerBlock);
  }
  
  
  protected abstract Buffer frontier();
  
  
  @Override
  public final int size() {
    return blocks.length * entriesPerBlock + frontier().position();
  }
  
  
  
  public final N last() {
    int size = size();
    if (size == 0)
      throw new NoSuchElementException();
    return get(size - 1);
  }
  
  
  
  /**
   * Ensures the frontier has remaining bytes for an write operation (add). If not,
   * then either a bigger frontier buffer is allocated and the state of the current
   * frontier is copied to it, <em>or</em> if the frontier buffer already contains
   * {@linkplain #entriesPerBlock} many entries, then the {@linkplain #blocks} array
   * is expanded with the current {@linkplain #frontierBuffer} buffer appended.
   * 
   * @return the {@linkplain #frontierBuffer}, ready for write
   */
  final Buffer ensureFrontier() {
    final var frontier = frontier();
    
    if (!frontier.hasRemaining()) {
      int currentCapacity = frontier.capacity();
      int maxAlloc = entriesPerBlock;
      if (currentCapacity ==  maxAlloc) {
        var blockArray = new Buffer[blocks.length + 1];
        blockArray[blocks.length] = frontier.flip();
        // NOTE: above flip() strictly NOT necessary since absolute index reads will still work
        for (int index = blocks.length; index-- > 0; )
          blockArray[index] = blocks[index];
        blocks = blockArray;
        return allocFrontier(maxAlloc);
      } else {
        assert currentCapacity < maxAlloc;
        // below underscores importance of choosing entrie-per-block wisely
        return reallocFrontier(Math.min(currentCapacity * 2, maxAlloc));
      }
    }
    
    return frontier;
  }
  
  
  abstract Buffer reallocFrontier(int newCap);
  
  abstract Buffer allocFrontier(int cap);
  
  
  /**
   * Returns the block containing the entry at the specified {@code index} (bounds checked here).
   * For a read operation (get).
   */
  final Buffer getBlock(int index) throws IndexOutOfBoundsException {
    int maxBlockIndex = blocks.length * entriesPerBlock;
    var frontier = frontier();
    Objects.checkIndex(index, maxBlockIndex + frontier.position());
    return
        index < maxBlockIndex ?
            blocks[index / entriesPerBlock] :
              frontier;
    
  }
  
  
  /**
   * Returns the offset into the buffer at which the entry at the specified {@code index}
   * can be found.
   */
  final int blockOffset(int index) {
    return index % entriesPerBlock;
  }
  
  
  
  
  //             E N D   B A S E   C L A S S   D E F
  
  
  
  
  
  
  
  public static class IntList extends NumberList<Integer> {
    
    private IntBuffer frontier;

    public IntList(int entriesPerBlock, int initEntryCap) {
      super(entriesPerBlock, initEntryCap);
      this.frontier = IntBuffer.allocate(initEntryCap);
    }
    

    @Override
    public Integer get(int index) {
      return getInt(index);
    }
    
    
    
    @Override
    public boolean add(Integer value) {
      addInt(value);
      return true;
    }
    
    public void addInt(int value) {
      ensureFrontier();
      frontier.put(value);
    }


    public int getInt(int index) {
      return ((IntBuffer) getBlock(index)).get(blockOffset(index));
    }


    @Override
    protected final IntBuffer frontier() {
      return frontier;
    }


    @Override
    Buffer reallocFrontier(int newCap) {
      frontier = IntBuffer.allocate(newCap).put(frontier.flip());
      return frontier;
    }


    @Override
    Buffer allocFrontier(int cap) {
      frontier = IntBuffer.allocate(cap);
      return frontier;
    }
  }
  
  
  
  public static class LongList extends NumberList<Long> {
    
    private LongBuffer frontier;

    public LongList(int entriesPerBlock, int initEntryCap) {
      super(entriesPerBlock, initEntryCap);
      this.frontier = LongBuffer.allocate(initEntryCap);
    }

    @Override
    protected final Buffer frontier() {
      return frontier;
    }

    @Override
    Buffer reallocFrontier(int newCap) {
      frontier = LongBuffer.allocate(newCap).put(frontier.flip());
      return frontier;
    }

    @Override
    Buffer allocFrontier(int cap) {
      frontier = LongBuffer.allocate(cap);
      return frontier;
    }

    @Override
    public Long get(int index) {
      return getLong(index);
    }
    
    @Override
    public boolean add(Long value) {
      addLong(value);
      return true;
    }
    
    
    public void addLong(long value) {
      ensureFrontier();
      frontier.put(value);
    }
    
    
    public long getLong(int index) {
      return ((LongBuffer) getBlock(index)).get(blockOffset(index));
    }
    
  }

}
