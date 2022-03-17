/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table;


import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

import io.crums.io.block.SortedBlock;
import io.crums.io.store.Sorted;
import io.crums.io.store.ks.Keystone;
import io.crums.io.store.ks.VolatileKeystone;
import io.crums.io.store.table.order.NaturalRowOrder;
import io.crums.io.store.table.order.RowOrder;
import io.crums.math.stats.SimpleSampler;
import io.crums.test.PerfProf;

/**
 * Marker class for a sorted table.
 * 
 * @see RowOrder
 * @see Table
 */
public class SortedTable extends Table implements Sorted {
  
  private final static Logger LOG = Logger.getLogger(SortedTable.class.getName());
  
  public enum Hint {
    BEFORE,
    AFTER;
  }
  
  
  private final RowOrder order;

  /**
   * Creates and returns a new instance. The instance's row count is inferred
   * from the file size, not from a keystone structure.
   * 
   * @param file
   *        the file channel positioned at the beginning of the table
   * @param rowSize
   *        the byte width of each row
   */
  public SortedTable(FileChannel file, int rowSize, RowOrder order) throws IOException {
    this(file, file.position(), rowSize, order);
  }

  /**
   * Creates and returns a new instance. The instance's row count is inferred
   * from the file size, not from a keystone structure.
   * 
   * @param file
   *        the file channel
   * @param zeroRowFileOffset
   *        the offset into the file channel where the table begins
   * @param rowSize
   *        the byte width of each row
   */
  public SortedTable(FileChannel file, long zeroRowFileOffset, int rowSize, RowOrder order)
      throws IOException {
    this(inMemoryKeystone(file, zeroRowFileOffset, rowSize), file, zeroRowFileOffset, rowSize, order);
  }



  /**
   * Creates and returns a new instance.
   * 
   * @param rowCount
   *        maintains the row count independent of the file length. If this object is backed
   *        in persistent storage, then {@linkplain Table#append(java.nio.ByteBuffer) appends}
   *        are atomic.
   * @param file
   *        the file channel
   * @param zeroRowFileOffset
   *        the offset into the file channel where the table begins
   * @param rowSize
   *        the byte width of each row
   */
  public SortedTable(
      Keystone rowCount, FileChannel file, long zeroRowFileOffset, int rowSize, RowOrder order)
          throws IOException {
    super(rowCount, file, zeroRowFileOffset, rowSize);
    if (order == null)
      order = NaturalRowOrder.INSTANCE;
    this.order = order;
  }
  
  
  
  
  /**
   * Copy constructor. Stick to read-only instances. Hard to reason otherwise.
   * Resources are reference counted. Meaning: the caller agrees to <em>close</em>
   * the instance once done with it.
   */
  public SortedTable(SortedTable copy) {
    super(copy);
    this.order = copy.order;
  }
  

  
  @Override
  public SortedTable clone() {
    return new SortedTable(this);
  }
  
  public final RowOrder order() {
    return order;
  }
  
  /**
   * Returns the row with the specified key if found. Use this only if you're not interested
   * in adjacent rows, row number, etc.
   * 
   * @param key semantics are defined by {@linkplain #order()}
   * 
   * @return the row if found; <tt>null</tt> o.w.
   */
  public ByteBuffer search(ByteBuffer key) throws IOException {
    Searcher searcher = newSearcher(Searcher.MIN_BUFFER_ROWS);
    return searcher.search(key) ? searcher.getHitRow() : null;
  }
  
  
  public Searcher newSearcher(int rowsInBuffer) throws IOException {
    if (!isOpen())
      throw new IllegalStateException("closed table: " + this);
    rowsInBuffer = (int) Math.max(
        Searcher.MIN_BUFFER_ROWS,
        Math.min(SortedTable.this.getRowCount(), rowsInBuffer));
    ByteBuffer buffer = ByteBuffer.allocate(rowsInBuffer * getRowWidth());
    return newSearcher(buffer, getRowWidth(), order);
  }
  
  
  
  /**
   * Override to customize the {@linkplain Searcher} implementation. Called by
   * {@linkplain #newSearcher(int)}.
   */
  protected Searcher newSearcher(ByteBuffer buffer, int rowWidth, RowOrder order) {
    return new Searcher(buffer, rowWidth, order);
  }
  
  
  
  
  
  
  
  
  private static Keystone inMemoryKeystone(
      FileChannel file, long zeroRowFileOffset, int rowSize)
          throws IOException {
    long byteLength = file.size() - zeroRowFileOffset;
    if (byteLength < 0)
      throw new IllegalArgumentException(
          "zero row offset is " + zeroRowFileOffset + "; file length is " + file.size());

    if (byteLength % rowSize != 0)
      LOG.warning(
          "Table length (" + byteLength + " bytes) not a multiple of row size (" + rowSize +
          " bytes). File position: " + file.position() + " .. Ignoring incomplete trailing row.");
    return new VolatileKeystone(byteLength / rowSize);
  }
  
  
  /**
   * TODO: use cached results intelligently.
   * 
   * @author Babak
   */
  public class Searcher {
    
    public final static int MIN_BUFFER_ROWS = 4;

    private final SortedBlock block;
    private long firstRowNumberInBlock;
    private ByteBuffer readOnlyBlockBufferView;
    
    
    private long excLo;
    private long excHi;
    
    /**
     * Row count snapshot.
     */
    private long rowCount;
    
    private long hitRowNumber;
    private int retrievedRowCount;
    
    private final PerfProf profiler;
    private final PerfProf blockSearchProfiler;
    private final SimpleSampler readOpStats;
    
    private int reads;
    
    
    
    /**
     * Creates a new instance with the given backing buffer. The buffer must be
     * at least {@linkplain #MIN_BUFFER_ROWS} rows wide. Also, if the buffer's capacity
     * is not a multiple of the the table's row width, it logs nagging warnings.
     */
    protected Searcher(ByteBuffer buffer, int rowWidth, RowOrder order) {
      if (buffer.isReadOnly())
        throw new IllegalArgumentException("buffer is read only");
      
      if (buffer.capacity() / rowWidth < MIN_BUFFER_ROWS)
        throw new IllegalArgumentException(
            "buffer too small: rowWidth=" + rowWidth + "; buffer capacity=" + buffer.capacity());
      this.block = new SortedBlock(buffer, rowWidth, order, true);
      profiler = new PerfProf();
      blockSearchProfiler = new PerfProf();
      readOpStats = new SimpleSampler();
    }
    
    
    public boolean search(ByteBuffer key) throws IOException {
      profiler.begin();
      boolean result = searchImpl(key);
      profiler.end();
      readOpStats.observe(reads);
      return result;
    }
    
    
    
    public final PerfProf getProfiler() {
      return profiler;
    }


    public final PerfProf getBlockSearchProfiler() {
      return blockSearchProfiler;
    }
    
    /**
     * Returns the statistics of the number of disk reads per search.
     */
    public SimpleSampler getReadOpStats() {
      return readOpStats;
    }
    
    
    public void clearProfilers() {
      profiler.clear();
      blockSearchProfiler.clear();
      readOpStats.clear();
    }


    private boolean searchImpl(ByteBuffer key) throws IOException {
      // TODO: the row may already be loaded in memory.. check the
      //       block first.. This may also gives us an opportunity to
      //       set a tighter boundary (excLo, excHi) -- although that
      //       has it drawbacks 
      excHi = firstRowNumberInBlock = hitRowNumber = rowCount = getRowCount();
      excLo = -1L;
      retrievedRowCount = reads = 0;
      if (rowCount == 0) {
        hitRowNumber = -1;
        return false;
      }
      
      // use the first cell in the block for sampling (reading) rows
      ByteBuffer rowData = block.buffer();
      rowData.clear().limit(getRowWidth());
      
      while (true) {
        
        long range = excHi - excLo - 1;
        
        ++reads;
        if (range <= block.cellCount()) {
          blockSearchProfiler.begin();
          boolean result = doBlockSearch(key, (int) range);
          blockSearchProfiler.end();
          return result;
        }
        
        // pick a pivot
        long pivot = (excHi + excLo) / 2;
        read(pivot, rowData);
        if (rowData.hasRemaining())
          throw new RuntimeException("assertion failure: rowData.hasRemaining(); " + rowData);
        rowData.flip();
        
        int comp = order.compare(key, rowData);
        
        if (comp < 0)
          excHi = pivot;
        else if (comp > 0)
          excLo = pivot;
        else {
          // we have a hit..
          hitRowNumber = firstRowNumberInBlock = pivot;
          retrievedRowCount = 1;
          return true;
        }
      }
      
    }

    private boolean doBlockSearch(ByteBuffer key, int range) throws IOException {
      if (range < 0)
        throw new RuntimeException("assertion failure: range=" + range);
      
      firstRowNumberInBlock = excLo + 1;
      
      ByteBuffer resultData = block.buffer();
      resultData.clear().limit(range * getRowWidth());
      
      read(firstRowNumberInBlock, resultData);
      
      int rowInBuffer = block.binarySearch(key, 0, range);
      if (rowInBuffer < 0)
        hitRowNumber = rowInBuffer - firstRowNumberInBlock; // miss
      else
        hitRowNumber = rowInBuffer + firstRowNumberInBlock; // hit
      
      retrievedRowCount = range;
      return hitRowNumber >= 0;
    }


    /**
     * Returns the row count snapshot at the time the search was started.
     */
    public long getRowCountSnapshot() {
      return rowCount;
    }


    /**
     * Returns the row index of the hit. If not found, it returns the insertion
     * point as a negative value: <tt>-<em>insertionPoint</em> - 1 </tt>.
     */
    public long getHitRowNumber() {
      return hitRowNumber;
    }

    public boolean isHit() {
      return hitRowNumber >= 0 && hitRowNumber < rowCount;
    }

    
    /**
     * Returns the hit row, if any.
     * <pre></tt>
        public ByteBuffer getHitRow() {
          return isHit() ? getRow(hitRowNumber) : null;
        }
     * </tt></pre>
     */
    public ByteBuffer getHitRow() {
      return isHit() ? getRow(hitRowNumber) : null;
    }


    /**
     * Returns the number of rows in the search buffer.
     */
    public int getRetrievedRowCount() {
      return retrievedRowCount;
    }


    /**
     * Returns the first retrieved row number in the search buffer.
     */
    public long getFirstRetrievedRowNumber() {
      return firstRowNumberInBlock;
    }


    /**
     * Returns the last retrieved row number in the search buffer (<em>exclusive</em>).
     */
    public long getLastRetrievedRowNumber() {
      return firstRowNumberInBlock + retrievedRowCount;
    }
    
    /**
     * Determines whether the given row is already loaded.
     * <pre><tt>
     *   return rowNumber < getLastRetrievedRowNumber() && rowNumber >= getFirstRetrievedRowNumber();
     * </tt></pre>
     * 
     * @see #getRow(long)
     */
    public boolean isRowInBuffer(long rowNumber) {
      return rowNumber < getLastRetrievedRowNumber() && rowNumber >= getFirstRetrievedRowNumber();
    }
    
    /**
     * Returns a read-only view of an already retrieved row, given its row number.
     * 
     * @param rowNumber
     *        row number in the range {@linkplain #getFirstRetrievedRowNumber()} (inclusive),
     *        {@linkplain #getLastRetrievedRowNumber()} (exclusive)
     *        
     * @throws IndexOutOfBoundsException
     *         if <tt>rowNumber</tt> is outside the retrieved range
     * 
     * @see #isRowInBuffer(long)
     * @see #copyRowInto(long, ByteBuffer)
     */
    public ByteBuffer getRow(long rowNumber) throws IndexOutOfBoundsException {
      return block.cell(toBlockIndex(rowNumber));
    }
    
    /**
     * Copies the contents of the given already retrieved <tt>rowNumber</tt> to the
     * specified <tt>buffer</tt>. The position of the given buffer is advanced by
     * the underlying {@linkplain #getTable() table}'s {@linkplain Table#getRowWidth() row width}.
     * <p/>
     * This is functionally equivalent to
     * <pre><tt>
     * Searcher seacher = ..
     * buffer.put(searcher.getRow(rowNumber));
     * </tt></pre>
     * except that it's marginally more efficient.
     * <p/>
     * 
     * @param rowNumber
     *        row number in the range {@linkplain #getFirstRetrievedRowNumber()} (inclusive),
     *        {@linkplain #getLastRetrievedRowNumber()} (exclusive)
     * @param buffer
     *        the buffer to which the row's contents will be put
     * @throws IndexOutOfBoundsException
     *         if <tt>rowNumber</tt> is outside the retrieved range
     * @throws BufferOverflowException
     *         if <tt>buffer</tt>'s remaining bytes is less than the underlying table's row width
     * 
     * @see #isRowInBuffer(long)
     * @see #getRow(long)
     */
    public void copyRowInto(long rowNumber, ByteBuffer buffer)
        throws IndexOutOfBoundsException, BufferOverflowException {
      block.copyCellInto(toBlockIndex(rowNumber), buffer);
    }
    
    
    
    /**
     * Returns the result of comparing the given <tt>key</tt> with contents
     * of the specified <tt>rowNumber</tt>.
     */
    public int compareToRetrievedRow(ByteBuffer key, long rowNumber) {
      return block.compareToCell(key, toBlockIndex(rowNumber));
    }
    
    
    
    private int toBlockIndex(long rowNumber) throws IndexOutOfBoundsException {
      long blockIndex = rowNumber - firstRowNumberInBlock;
      if (blockIndex < 0 || blockIndex >= retrievedRowCount)
        throw new IndexOutOfBoundsException("rowNumber: " + rowNumber);
      return (int) blockIndex;
    }
    
    
    
    /**
     * Returns the shared, read-only view of the specified rows already loaded in the
     * search buffer. Since it's shared, the returned buffer is for immediate consumption:
     * it must be used before the next invocation of this method or of {@linkplain #search(ByteBuffer)}.
     * 
     * @param rowNumber
     *        row number in the range {@linkplain #getFirstRetrievedRowNumber()} (inclusive),
     *        {@linkplain #getLastRetrievedRowNumber()} (exclusive)
     * @param count
     *        the number of rows to return in the buffer
     * @return
     *        the shared read-only view of the search buffer, its remaining bytes the contents
     *        of the specified rows
     *        
     */
    public ByteBuffer getRows(long rowNumber, int count) throws IndexOutOfBoundsException {
      
      if (count < 1)
        throw new IllegalArgumentException("count: " + count);

      long blockIndex = rowNumber - firstRowNumberInBlock;
      long lastBlockIndex = blockIndex + count; // exc.
      
      if (blockIndex < 0 || lastBlockIndex - blockIndex > retrievedRowCount)
        throw new IndexOutOfBoundsException("rowNumber=" + rowNumber + "; count=" + count);
      
      int blockPosition = (int) blockIndex * getRowWidth();
      int blockLimit = blockPosition + count * getRowWidth();
      
      if (readOnlyBlockBufferView == null)
        readOnlyBlockBufferView = block.buffer().asReadOnlyBuffer();
      
      readOnlyBlockBufferView.limit(blockLimit).position(blockPosition);
      
      return readOnlyBlockBufferView;
    }
    
    
    public SortedTable getTable() {
      return SortedTable.this;
    }
  }
  
}
