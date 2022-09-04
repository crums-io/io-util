/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.buffer;


import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import io.crums.util.Maps;
import io.crums.util.Strings;

/**
 * A partition with named parts. This is not envisioned with very many parts
 * (tho the parts themselves can be big), so there's no attempt at say
 * prefix compressing the keys.
 */
public class NamedParts extends Partitioning {
  
  /**
   * The empty instance.
   */
  public final static NamedParts EMPTY = new NamedParts();
  
  /**
   * Maximum name length.
   */
  public final static int MAX_NAME_LEN = 0xffff;
  
  
  
  
  
  

  private final List<String> names;
  private final Map<String, Integer> indexMap;

  /**
   * Regular constructor.
   * 
   * @param block with capacity &ge; 1 (you're mine now: position and limit don't matter)
   * @param names   unique list of part names (of length {@code sizes})
   * @param sizes list of non-negative sizes defining the boundaries between adjacent partitions.
   *              Must sum to the {@code block.capacity()}
   * @see #EMPTY
   */
  public NamedParts(ByteBuffer block, List<String> names, List<Integer> sizes) {
    super(block, sizes);
    this.names = sealNames(names);
    this.indexMap = createIndexMap(names);
  }

  /**
   * Promotion constructor.
   * 
   * @param promoted
   * @param names   unique list of part names (of length {@code promoted.getParts()})
   */
  public NamedParts(List<String> names, Partitioning promoted) {
    super(promoted);
    this.names = sealNames(names);
    this.indexMap = createIndexMap(names);
  }
  

  /**
   * Copy constructor. Protected scope, cuz instances of this class are immutable
   * and no reason to copy--unless you're a subclass.
   */
  protected NamedParts(NamedParts copy) {
    super(copy);
    this.names = copy.names;
    this.indexMap = copy.indexMap;
  }
  


  
  private NamedParts(Partitioning readOnlyCopy, NamedParts copy) {
    super(readOnlyCopy);
    this.names = copy.names;
    this.indexMap = copy.indexMap;
  }
  
  
  private NamedParts() {
    super(NULL);
    this.names = Collections.emptyList();
    this.indexMap = Collections.emptyMap();
  }
  

  
  private List<String> sealNames(List<String> names) {
    Objects.requireNonNull(names, "null names");
    if (names.size() != getParts())
      throw new IllegalArgumentException(
          "names/parts mismatch: " + names.size() + "/" + getParts());
    return List.copyOf(names);
  }
  
  private Map<String,Integer> createIndexMap(List<String> names) {
    final int count = names.size();
    if (count != getParts())
      throw new IllegalArgumentException(
          "names/parts mismatch: " + count + "/" + getParts());
    var indexMap = new HashMap<String,Integer>(count);
    for (int index = 0; index < count; ++index) {
      var name = names.get(index);
      if (name.length() > MAX_NAME_LEN)
        throw new IllegalArgumentException(
            "name[" + index +  "] '" + name.substring(0, 16) + "..' too long (" +
            name.length() + ")");
      if (indexMap.put(names.get(index), index) != null)
        throw new IllegalArgumentException(
            "duplicate key '" + names.get(index) + "' in names: " + names);
    }
    return indexMap;
  }
  
  
  
  
  
  
  
  
  
  public ByteBuffer part(String name) throws NoSuchElementException {
    Integer index = indexMap.get(name);
    if (index == null)
      throw new NoSuchElementException(name);
    return getPart(index);
  }
  
  
  public Optional<ByteBuffer> getPart(String name) {
    Integer index = indexMap.get(name);
    if (index == null)
      return Optional.empty();
    return Optional.of(getPart(index));
  }
  
  
  public List<String> getPartNames() {
    return names;
  }
  
  
  
  public Map<String,ByteBuffer> asMap() {
    return Maps.mapValues(indexMap, index -> getPart(index));
  }

  @Override
  public NamedParts readOnlyView() {
    return isReadOnly() ? this : new NamedParts(super.readOnlyView(), this);
  }

  /**
   * <h4>Performance Note</h4>
   * <p>Avoid if possible. Expensive. Write the header directly.</p>
   * {@inheritDoc}
   */
  @Override
  public int serialHeaderSize() {
    final int count = getParts();
    int bytes = 4 + count * 8;
    for (int index = 0; index < count; ++index)
      bytes += names.get(index).getBytes(Strings.UTF_8).length;
    return bytes;
  }

  @Override
  public ByteBuffer writeHeaderTo(ByteBuffer out) {
    final int count = getParts();
    out.putInt(count);
    for (int index = 0; index < count; ++index) {
      var name = names.get(index);
      var bytes = name.getBytes(Strings.UTF_8);
      out.putInt(getPartSize(index));
      out.putInt(bytes.length);
      out.put(bytes);
    }
    return out;
  }
  
  
  /**
   * <h4> Interface Description </h4>
   * <p>
   * {@inheritDoc}
   * <p>
   * <h4> Implementation </h4>
   * <p>
   * Allows 2-bytes per UT8-character. For ASCII names this is about twice
   * the actual space needed. If there are many names with more code-points
   * than their character-lengths, then this estimate may prove too small.
   * <p>
   * 
   * 
   */
  @Override
  public int estimateSize() {
    int bytes = names.stream().map(name -> name.length()).reduce(0, Integer::sum);
    bytes *= 2;
    bytes += names.size()*9 + 4 + getBlockSize();
    if (bytes < 0)
      throw new IllegalStateException("bytes overflow: " + bytes);
    return bytes;
  }

  
  
  
  
  
  
  // - - -  S T A T I C   L O A D   M E T H O D S  - - -
  
  
  
  
  
  
  /**
   * Loads a read-only instance from its {@linkplain io.crums.io.Serial Serial} byte representation.
   * 
   * @return {@linkplain #load(ByteBuffer, boolean) load(serialForm, true)}
   */
  public static NamedParts load(ByteBuffer serialForm)
      throws BufferUnderflowException, IllegalArgumentException {
    return load(serialForm, true);
  }
  
  
  
  /**
   * Loads an instance from the given buffer.
   * 
   * @param serialForm buffer
   * @param readOnly   if {@code true} then the block is ensured to be read-only
   * 
   * @throws IllegalArgumentException if we read nonsense
   * @throws BufferUnderflowException if the nonsense we read directed us to read
   *         beyond the end of the buffer
   */
  public static NamedParts load(ByteBuffer serialForm, boolean readOnly)
      throws BufferUnderflowException, IllegalArgumentException {
    
    // begin header
    final int partCount = Objects.requireNonNull(serialForm, "null serialForm").getInt();
    if (partCount == 0)
      return EMPTY;
    
    if (partCount < 0)
      throw new IllegalArgumentException(
          "negative part count: " + partCount + " from " + serialForm);
    
    var sizes = new ArrayList<Integer>(partCount);
    var names = new ArrayList<String>(partCount);

    int totalSize = 0;
    for (int index = 0; index < partCount; ++index) {
      int size = serialForm.getInt();
      totalSize += size;
      if (size < 0)
        throw new IllegalArgumentException(
            "negative paritition size: " + size + " at [" + index + ":" + partCount + "]");
      sizes.add(size);
      byte[] nameBytes;
      {
        int nbSize = serialForm.getInt();
        
        if (nbSize < 0)
          throw new IllegalArgumentException(
              "negative name byte-size: " + nbSize + " at [" + index + ":" + partCount + "]");
        if (nbSize > MAX_NAME_LEN)throw new IllegalArgumentException(
            "name byte-size " + nbSize + " exceeds maximum at [" + index + ":" + partCount + "]");
        
        nameBytes = new byte[nbSize];
        serialForm.get(nameBytes);
      }
      names.add(new String(nameBytes, Strings.UTF_8));
    }
    // end header

    var block = BufferUtils.slice(serialForm, totalSize);
    
    if (readOnly && !block.isReadOnly())
      block = block.asReadOnlyBuffer();
    
    return new NamedParts(block, names, sizes);
  }
  
  
  
  
  public static NamedParts createInstance(Map<String, ByteBuffer> map) {
    if (map.isEmpty())
      return NamedParts.EMPTY;
    int blockSize = map.values().stream().map(ByteBuffer::remaining).reduce(0, Integer::sum);
    ByteBuffer block = ByteBuffer.allocate(blockSize);
    List<Integer> sizes = new ArrayList<>(map.size());
    List<String> names = new ArrayList<>(map.size());
    for (var entry : map.entrySet()) {
      var buf = entry.getValue().duplicate();
      names.add(entry.getKey());
      sizes.add(buf.remaining());
      block.put(buf);
    }
    assert !block.hasRemaining();
    block.flip();
    
    return new NamedParts(block.asReadOnlyBuffer(), names, sizes);
  }
  

}

















