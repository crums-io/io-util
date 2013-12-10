/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

import org.apache.log4j.Logger;

import com.gnahraf.io.buffer.Covenant;
import com.gnahraf.io.store.table.SortedTableBuilder;
import com.gnahraf.io.store.table.Table;
import com.gnahraf.io.store.table.order.RowOrder;

/**
 * 
 * @author Babak
 */
public class WriteAheadTableBuilder extends SortedTableBuilder implements Channel {
  
  private final static Logger LOG = Logger.getLogger(WriteAheadTableBuilder.class);
  
  private final File writeAheadFile;
  private final Table writeAheadTable;

  public WriteAheadTableBuilder(
      int rowWidth, RowOrder order, File writeAheadFile)
          throws IOException {
    super(rowWidth, order);
    this.writeAheadFile = writeAheadFile;
    if (writeAheadFile == null)
      throw new IllegalArgumentException("null writeAheadFile");
    boolean recover = writeAheadFile.exists();
    FileChannel tableChannel = new RandomAccessFile(writeAheadFile, "rw").getChannel();
    try {
      if (recover) {
        this.writeAheadTable = Table.loadInstance(tableChannel, rowWidth);
        long writeAheadRowCount = writeAheadTable.getRowCount();
        if (writeAheadRowCount > 0) {
          LOG.info("loading uncommitted updates from WAL " + writeAheadFile.getPath());
          ByteBuffer rows = allocateRows((int) writeAheadRowCount);
          rows.clear();
          writeAheadTable.read(0, rows);
          rows.flip();
          final int finalLimit = rows.limit();
          rows = rows.asReadOnlyBuffer();
          int pos = 0;
          int limit = pos + getRowWidth();
          
          while (pos < finalLimit) {
            rows.position(pos).limit(limit);
            overwrite(rows.slice());
            pos = limit;
            limit += getRowWidth();
          }
        }
      } else {
        this.writeAheadTable = Table.newEmptyInstance(tableChannel, rowWidth);
      }
    } catch (IOException iox) {
      // we're bailing.. so close the write-ahead table before throwing the wrench
      tableChannel.close();
      throw iox;
    }
  }

  @Override
  protected void preInsertion(ByteBuffer rows) throws IOException {
    rows.mark();
    writeAheadTable.append(rows);
    rows.reset();
  }

  public final File getWriteAheadFile() {
    return writeAheadFile;
  }

  @Override
  public boolean isOpen() {
    return writeAheadTable.isOpen();
  }

  @Override
  public void close() throws IOException {
    writeAheadTable.close();
  }
  
  public long getWalSize() throws IOException {
    return writeAheadTable.getRowCount() * getRowWidth();
  }
  
  
  public void writeAheadButRemove(ByteBuffer row) throws IOException {
    if (!sortedView.remove(row))
      throw new IllegalArgumentException("attempt to remove nonexistent row " + row);
    writeAheadTable.append(row);
  }

  /**
   * 
   */
  @Override
  public void clear() {
    if (isOpen())
      throw new IllegalStateException("attempt to clear open instance");
    super.clear();
  }
  
  
  /**
   * Flushes the contents of this builder in sorted form, but does not <em>clear</em>
   * the instance.
   * 
   * @param file
   * @throws IOException
   */
  public void flush(GatheringByteChannel file) throws IOException {
    super.flush(file, false);
  }

  /**
   * Not supported because we want to guarantee you only ever {@linkplain #clear() clear}
   * an instance after it has been {@linkplain #close() close}d.
   * 
   * @see #flush(GatheringByteChannel)
   */
  @Override
  public void flush(GatheringByteChannel file, boolean clear) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }
  
  

}
