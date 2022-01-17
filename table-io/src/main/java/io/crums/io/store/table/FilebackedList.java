/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.store.table;


import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.AbstractList;
import java.util.Objects;
import java.util.RandomAccess;

/**
 * List adapter for a <tt>Table</tt>.
 */
public class FilebackedList extends AbstractList<ByteBuffer> implements Channel, RandomAccess {
  
  protected final Table table;
  private final boolean autoFlush;

  /**
   * Creates an instance with the given backing table. Autoflush is by default off.
   */
  public FilebackedList(Table table) {
    this(table, false);
  }


  /**
   * Creates an instance with the given backing table.
   * 
   * @param table     the backing fixed-width table
   * @param autoFlush if <tt>true</tt>, then autoflush is on
   */
  public FilebackedList(Table table, boolean autoFlush) {
    this.table = Objects.requireNonNull(table, "table");
    this.autoFlush = autoFlush;
  }

  

  /**
   * <p>Determines whether the backing table is open.</p>
   * 
   * {@inheritDoc}
   */
  @Override
  public boolean isOpen() {
    return table.isOpen();
  }

  /**
   * <p>Closes the backing table.</p>
   * 
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    table.close();
  }
  
  

  @Override
  public boolean add(ByteBuffer e) {
    if (e.remaining() != elementWidth())
      throw new IllegalArgumentException(
          "remaining bytes " + e.remaining() + " != element width " + elementWidth());
    try {
      e.mark();
      table.append(e);
      e.reset();
      if (autoFlush)
        table.flush();
    } catch (IOException iox) {
      throw new UncheckedIOException("on add to " + table, iox);
    }
    return true;
  }


  
  @Override
  public ByteBuffer get(int index) {
    ByteBuffer out = allocateRowOut();
    try {
      table.read(index, out);
    } catch (IOException iox) {
      throw new UncheckedIOException("failed to read index " + index + " from table " + table, iox);
    }
    return out.flip();
  }

  @Override
  public int size() {
    
    long count;
    try {
      count = table.getRowCount();
    } catch (IOException iox) {
      throw new UncheckedIOException("failed to read table size: " + table, iox);
    }
    
    if (count > Integer.MAX_VALUE)
      throw new IllegalStateException("row count overflow (not representable as int): " + count);
    
    return (int) count;
  }
  
  
  /**
   * Returns the number of bytes in each element.
   */
  public final int elementWidth() {
    return table.getRowWidth();
  }
  
  
  public final boolean elementHasArray() {
    return allocateRowOut().hasArray();
  }
  
  
  protected ByteBuffer allocateRowOut() {
    return ByteBuffer.allocate(table.getRowWidth());
  }
  

  /**
   * Creates and returns an instance in read-write mode; if it doesn't exist on the file system, creates it.
   * 
   * @param file      file path to the backing table
   * @param rowSize   bytes per row
   * @param autoFlush if <tt>true</tt>, then on every add forces a flush to the file system. If <tt>false</tt>,
   *                  then the last add may be lost on abnormal shutdown.
   */
  public static FilebackedList createInstance(File file, int rowSize, boolean autoFlush) throws IOException {
    return createInstanceImpl(file, rowSize, false, autoFlush);
  }
  
  
  public static FilebackedList createReadOnlyInstance(File file, int rowSize) throws IOException {
    return createInstanceImpl(file, rowSize, true, false);
  }
  

  /**
   * Creates and returns an instance in read-write mode; if it doesn't exist on the file system, creates it.
   * 
   * @param file      file path to the backing table
   * @param rowSize   bytes per row
   */
  public static FilebackedList createInstance(File file, int rowSize) throws IOException {
    return createInstanceImpl(file, rowSize, false, false);
  }

  /**
   * Creates and returns an instance; if it doesn't exist on the file system, creates it.
   * 
   * @param file      file path to the backing table
   * @param rowSize   bytes per row
   * @param readOnly  if <tt>true</tt>, then the list is opened in read-only mode. (Obviously, the backing
   *                  table must already exist on the file system.)
   */
  public static FilebackedList createInstanceImpl(File file, int rowSize, boolean readOnly, boolean autoFlush) throws IOException {
    Table table = Table.createInstance(file, rowSize, readOnly);
    return new FilebackedList(table, autoFlush);
  }

}
