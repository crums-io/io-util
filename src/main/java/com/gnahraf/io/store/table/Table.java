/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.logging.Logger;

import com.gnahraf.io.FileUtils;
import com.gnahraf.io.channels.ChannelUtils;
import com.gnahraf.io.channels.SharedChannel;
import com.gnahraf.io.store.ks.CachingKeystone;
import com.gnahraf.io.store.ks.Keystone;
import com.gnahraf.io.store.ks.RollingKeystone;
import com.gnahraf.io.store.ks.VolatileKeystone;


/**
 * A serialized fixed width table supporting random access to its rows. This
 * structure does not delve into defining multiple columns. You can build that
 * yourself; there's only one column in this implementation.
 * 
 * <h3>Concurrent Access</h3>
 * 
 * The underlying <tt>FileChannel</tt>'s position is never modified.
 * Consequently, if the <tt>FileChannel</tt>'s implementation is itself
 * thread-safe, then concurrent reads on an instance of this class are also
 * safe. Not only that, concurrent readers with one append-only writer would
 * also be safe. I say <em>would</em>, because I vaguely recall a while back
 * discovering some crap standard API code that seemed to violate
 * <tt>FileChannel</tt>'s own spec: it would be changing the file position and then
 * setting it back.
 * 
 * <h3>Instance Duplicates</h3>
 * 
 * Copy constructor semantics have special meaning. Instances share the underlying
 * <tt>FileChannel</tt> while still implementing the <tt>Channel</tt> interface methods
 * <tt>isOpen()</tt> and <tt>close()</tt> on the copied instance. This is done by
 * maintained a reference count on the copied instances.  So the contract here is that
 * every user agrees to do the right thing: namely close their instance when they're done using
 * it.
 * 
 * 
 * @author Babak
 */
public class Table implements Channel {
  
  private final static Logger LOG = Logger.getLogger(Table.class.getName());
  
  protected final Object filePositionLock;

  private final Keystone rowCount;
  private final FileChannel file;
  private final SharedChannel sharedFile;
  private final long zeroRowFileOffset;
  private final int rowSize;


  /**
   * Creates a new instance by loading an already serialized table. There's no
   * constructor for writing a new empty instance to a file: to write a new
   * instance, you need only write a keystone with value zero, and pass it into
   * this constructor.
   * 
   * @param rowCount
   *          the keystone structure that holds the row count. Usually this
   *          keystone is written in the same file in which the table rows are
   *          written.
   * @param file
   *          the file channel
   * @param zeroRowFileOffset
   *          the file offset at which the first (zeroth) row starts
   * @param rowSize
   *          the size of each row in bytes (&gt; 0)
   */
  public Table(Keystone rowCount, FileChannel file, long zeroRowFileOffset, int rowSize) throws IOException {
    this.filePositionLock = new Object();
    this.rowCount = rowCount;
    this.file = file;
    this.sharedFile = new SharedChannel(file);
    this.zeroRowFileOffset = zeroRowFileOffset;
    this.rowSize = rowSize;

    if (zeroRowFileOffset < 0)
      throw new IllegalArgumentException("zeroRowFileOffset: " + zeroRowFileOffset);
    checkArgs(file, rowSize);

    long rows = checkRowCount(rowCount);
    if (file.size() < zeroRowFileOffset + rows * rowSize)
      throw new IOException("file size (" + file.size() + " bytes) too small; zeroRowFileOffset is " + zeroRowFileOffset + "; row count is " + rows + "; and row size is " + rowSize);
  }
  
  
  /**
   * Copy constructor. Safe for read-only. Avoid write mode: hard to think thru.
   */
  public Table(Table copy) {
    this.filePositionLock = copy.filePositionLock;
    this.rowCount = copy.rowCount;
    this.file = copy.file;
    this.sharedFile = new SharedChannel(copy.sharedFile);
    this.zeroRowFileOffset = copy.zeroRowFileOffset;
    this.rowSize = copy.rowSize;
  }
  
  
  
  
  
  


  /**
   * Sets the data for the given <tt>row</tt>.
   * 
   * @param row
   *          the row number (between zero and the {@linkplain #getRowCount()
   *          row count}, inclusive)
   * @param rowData
   *          buffer having 1 or multiple rows of data ready for writing
   * @throws IOException
   */
  public void set(long row, ByteBuffer rowData) throws IOException {
    checkOpen();
    if (row < 0)
      throw new IllegalArgumentException("row: " + row);
    long currentCount = rowCount.get();
    if (row > currentCount)
      throw new IllegalArgumentException("row (" + row + ") is greater than current row count (" + currentCount + ")");
    int numRowsInBuffer = numRowsInBuffer(rowData);
    long rowOffsetInFile = rowOffset(row);
    ChannelUtils.writeRemaining(file, rowOffsetInFile, rowData);
    long nextRow = row + numRowsInBuffer;
    if (nextRow > currentCount)
      rowCount.set(nextRow);
  }


  /**
   * Appends one or more rows to the table and returns the index (row number) of
   * the first row appended.
   * 
   * @param rowData
   *          buffer containing data for one or more rows that are to be
   *          written. The number of remaining bytes must be a multiple of the
   *          {@linkplain #getRowWidth() row size}.
   */
  public long append(ByteBuffer rowData) throws IOException {
    checkOpen();
    int newRows = numRowsInBuffer(rowData);
    long currentRowCount = rowCount.get();
    long rowOffsetInFile = rowOffset(currentRowCount);
    ChannelUtils.writeRemaining(file, rowOffsetInFile, rowData);
    long newRowCount = rowCount.increment(newRows);
    if (newRowCount != currentRowCount + newRows)
      throw new IOException("sanity check failure: " + newRowCount + " != " + currentRowCount + " + " + newRows);
    return currentRowCount;
  }
  
  
  /**
   * Appends one or more rows to the table and returns the index (row number) of
   * the first row appended. Note this method temporarily modifies the file's position
   * 
   * @param rows
   *        non-zero length array of row buffers. The total number of remaining bytes
   *        must be an exact, non-zero multiple of the table's {@linkplain #getRowWidth() rowWidth}.
   * 
   * @throws NullPointerException
   *         if the <tt>rows</tt> array is <tt>null</tt>, or if any of its elements is <tt>null</tt>
   * @throws IllegalArgumentException
   *         if the total number of remaining bytes in the given <tt>rows</tt> is not a non-zero
   *         multiple of the table's {@linkplain #getRowWidth() rowWidth}
   */
  public long append(ByteBuffer[] rows) throws IOException {
    long newRows = numRowsInBuffers(rows);

    checkOpen();
    synchronized (filePositionLock) {
      long firstRowNumber = rowCount.get();
      long rowOffsetInFile = rowOffset(firstRowNumber);
      file.position(rowOffsetInFile);
      ChannelUtils.writeRemaining(file, rows);
      long newRowCount = rowCount.increment(newRows);
      if (newRowCount != firstRowNumber + newRows)
        throw new IOException("sanity check failure: " + newRowCount + " != " + firstRowNumber + " + " + newRows);
      return firstRowNumber;
    }
  }
  
  
  
  public void flush() throws IOException {
    checkOpen();
    file.force(false);
  }


  /**
   * Reads a block of one or more rows starting at the given <tt>row</tt>
   * number.
   * 
   * @param row
   *          the zero-based row number
   * @param rowData
   *          the buffer to which the row data is to be copied to. The number of
   *          remaining bytes must be a multiple of the
   *          {@linkplain #getRowWidth() row size}.
   */
  public void read(long row, ByteBuffer rowData) throws IOException {
    if (row < 0)
      throw new IllegalArgumentException("row: " + row);
    int rows = numRowsInBuffer(rowData);
    if (row + rows > rowCount.get())
      throw new IllegalArgumentException(
          "Overflow: read request beyond end of table. " + "Row number is " + row +
          "; number of rows to copy into rawData buffer is " + rows +
          "; current row count is " + rowCount.get());
    long fileOffset = rowOffset(row);
    checkOpen();
    ChannelUtils.readRemaining(file, fileOffset, rowData);
  }
  
  
  /**
   * Transfers (copies) a block of rows to the given <tt>target</tt>. This should
   * be far more efficient than using a secondary work buffer to first read and
   * then write data.
   * 
   * @param row
   *        the starting row number
   * @param count
   *        the number of rows to copy
   * @param target
   *        where the data is copied to
   */
  public void transferRows(long row, long count, WritableByteChannel target) throws IOException {
    if (noopOrCheckBlockOpArgs(row, count, this))
      return;
    long offset = rowOffset(row);
    long blockLength = count * rowSize;
    checkOpen();
    ChannelUtils.transferBytes(file, target, offset, blockLength);
  }
  
  
  /**
   * Appends rows from the given <tt>source</tt> table. This is likely a lot
   * faster than copying the rows to a buffer and then appending the buffer
   * in this instance.
   * 
   * @param source
   * @param row
   *        the row number in the source table
   * @param count
   *        the number of rows in the source table to append here
   */
  public void appendRows(Table source, long row, long count) throws IOException {
    if (noopOrCheckBlockOpArgs(row, count, source))
      return;
    if (source == this)
      throw new IllegalArgumentException("attempt to append self to self: " + this);
    if (source.getRowWidth() != getRowWidth())
      throw new IllegalArgumentException(
          "row width mismatch. Expected " + rowSize + "; actual was " + source.getRowWidth());
    long offset = source.rowOffset(row);
    long blockLength = count * rowSize;
    
    checkOpen();
    synchronized (filePositionLock) {
      file.position(rowOffset(getRowCount()));
      ChannelUtils.transferBytes(source.file, file, offset, blockLength);
      rowCount.increment(count);
    }
  }
  
  
  private static boolean noopOrCheckBlockOpArgs(
      long row, long count, Table table) throws IOException {
    if (count <= 0) {
      if (count == 0)
        return true;
      throw new IllegalArgumentException("count: " + count);
    }
    if (table == null)
      throw new IllegalArgumentException("null table");
    if (row < 0)
      throw new IllegalArgumentException("row: " + row);
    if (row + count > table.rowCount.get())
      throw new IllegalArgumentException(
          "Overflow: read request beyond end of table. Row number is " + row +
          "; number of rows to copy into rawData buffer is " + count +
          "; current row count is " + table.rowCount.get() + "; table..\n" + table);
    return false;
  }


  /**
   * Returns the number of bytes of data each row holds.
   */
  public final int getRowWidth() {
    return rowSize;
  }


  /**
   * Returns the row count.
   */
  public long getRowCount() throws IOException {
    return rowCount.get();
  }
  
  
  public final boolean isEmpty() throws IOException {
    return getRowCount() == 0;
  }
  
  
  public long trimToSize() throws IOException {
    long size = rowOffset(getRowCount());
    file.truncate(size);
    return size;
  }


  private long rowOffset(long row) {
    return zeroRowFileOffset + row * rowSize;
  }


  private int numRowsInBuffer(ByteBuffer buffer) {
    if (buffer == null)
      throw new IllegalArgumentException("null buffer");
    int size = buffer.remaining();
    if (size == 0)
      throw new IllegalArgumentException("empty buffer");
    if (size % rowSize != 0)
      throw new IllegalArgumentException(
          "buffer size (" + size + " bytes) not a multiple of row size (" + rowSize + " bytes)");
    return size / rowSize;
  }
  
  
  
  private int numRowsInBuffers(ByteBuffer[] buffers) {
    long remaining = 0;
    for (int index = buffers.length; index-- > 0; ) {
      remaining += buffers[index].remaining();
    }
    if (remaining == 0 || remaining % getRowWidth() != 0)
      throw new IllegalArgumentException(
          "illegal remaining bytes in rows: " + Arrays.asList(buffers).toString() + "; remaining = " + remaining);
    return (int) (remaining / getRowWidth());
  }


  //  S T A T I C   M E M B E R S

  /**
   * Loads and returns a saved instance from the file system.
   * 
   * @param file
   *          the file channel, positioned at the beginning of the table
   * @param rowSize
   *          the byte width of each row
   */
  public static Table loadInstance(FileChannel file, int rowSize) throws IOException {
    return newInstanceImpl(file, rowSize, false);
  }
  
  /**
   * Creates and returns an instance in read-write mode; if it doesn't exist on the file system, creates it.
   * 
   * @param file      file path
   * @param rowSize   bytes per row
   */
  public static Table createInstance(File file, int rowSize) throws IOException {
    return createInstance(file, rowSize, false);
  }
  
  /**
   * Creates and returns an instance; if it doesn't exist on the file system, creates it.
   * 
   * @param file      file path
   * @param rowSize   bytes per row
   * @param readOnly  if <tt>true</tt>, then the table is opened in read-only mode. (Obviously, the
   *                  table must already exist on the file system.)
   */
  public static Table createInstance(File file, int rowSize, boolean readOnly) throws IOException {
    if (rowSize < 1)
      throw new IllegalArgumentException("rowSize: " + rowSize);
    
    String mode = readOnly ? "r" : "rw";
    boolean load = file.exists();
    if (readOnly && !load)
      throw new IllegalArgumentException("cannot load non-existent file in read-only mode: " + file);
    
    if (!load)
      FileUtils.ensureDir(file.getParentFile());

    @SuppressWarnings("resource")
    FileChannel ch = new RandomAccessFile(file, mode).getChannel();
    ch.position(0);
    
    return load ? loadInstance(ch, rowSize) : newEmptyInstance(ch, rowSize);
    
  }
  
  
  
  /**
   * Loads and returns a new or saved instance from the file system. In this type of table
   * there's no keystone structure holding the number of rows in the table: the row count is
   * inferred by the table's byte length. For write-once tables, this is a better choice.
   *
   * 
   * @param file
   *          the file channel, positioned at the beginning of the table
   * @param rowSize
   *          the byte width of each row
   *          
   * @throws IOException
   */
  public static Table newSansKeystoneInstance(FileChannel file, int rowSize) throws IOException {
    checkArgs(file, rowSize);
    long byteLength = file.size() - file.position();
    if (byteLength % rowSize != 0)
      LOG.warning(
          "Table length (" + byteLength + " bytes) not a multiple of row size (" + rowSize +
          " bytes). File position: " + file.position() + " .. Ignoring incomplete trailing row.");
    Keystone rowCount = new VolatileKeystone(byteLength / rowSize);
    return new Table(rowCount, file, file.position(), rowSize);
  }


  /**
   * Creates and returns an empty instance on the file system.
   * 
   * @param file
   *          the file channel, positioned at the beginning of the table
   * @param rowSize
   *          the byte width of each row
   */
  public static Table newEmptyInstance(FileChannel file, int rowSize) throws IOException {
    return newInstanceImpl(file, rowSize, true);
  }


  private static Table newInstanceImpl(FileChannel file, int rowSize, boolean empty) throws IOException {
    checkArgs(file, rowSize);
    long position = file.position();
    Keystone rowCount;
    if (empty)
      rowCount = new RollingKeystone(file, position, 0);
    else
      rowCount = new RollingKeystone(file, position);
    rowCount = new CachingKeystone(rowCount);
    position += rowCount.size();
    return new Table(rowCount, file, position, rowSize);
  }


  private static void checkArgs(FileChannel file, int rowSize) throws ClosedChannelException {
    if (file == null)
      throw new IllegalArgumentException("null file channel");
    if (!file.isOpen())
      throw new ClosedChannelException();
    if (rowSize < 1)
      throw new IllegalArgumentException("rowSize: " + rowSize);
  }


  private static long checkRowCount(Keystone rowCount) throws IOException {
    if (rowCount == null)
      throw new IllegalArgumentException("null rowCount");
    long rows = rowCount.get();
    if (rows < 0)
      throw new IOException("negative row count (" + rows + ") in keystone");
    return rows;
  }
  
  
  protected final void checkOpen() throws ClosedChannelException {
    if (!sharedFile.isOpen())
      throw new ClosedChannelException();
  }


  @Override
  public boolean isOpen() {
    return sharedFile.isOpen();
  }


  @Override
  public void close() throws IOException {
    rowCount.commit();
    sharedFile.close();
  }

}

