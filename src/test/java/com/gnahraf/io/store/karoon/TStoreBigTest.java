/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.junit.Test;

import com.gnahraf.io.store.karoon.TStoreConfig.Builder;
import com.gnahraf.io.store.karoon.merge.MergePolicy;
import com.gnahraf.io.store.karoon.merge.MergePolicyBuilder;
import com.gnahraf.io.store.table.del.DeleteCodec;
import com.gnahraf.io.store.table.del.MagicNumDeleteCodec;
import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.io.store.table.order.RowOrders;
import com.gnahraf.test.RandomSequence;
import com.gnahraf.test.TestMethodHarness;

/**
 * 
 * @author Babak
 */
public class TStoreBigTest extends TestMethodHarness {
  
  public final static String RUN_PROPERTY = TStoreBigTest.class.getSimpleName();

  private final static RowOrder order = RowOrders.LONG_ORDER;
  private final static long CODEC_MAGIC = -1;
  private final static DeleteCodec deleteCodec = MagicNumDeleteCodec.newLongInstance(8, CODEC_MAGIC);

  @Test
  public void test64ByteRow() throws IOException {
    initUnitTestDir(new Object() { });
    if (!checkEnabled())
      return;
    
    log.info("+----------------------------------------------");
    log.info("|");
    log.info("| " + getMethod() + " stress test");
    log.info("|");
    log.info("+----------------------------------------------");
    
    final int rowWidth = 64;
    final long rowCount = 1024 * 1024;
    final long randomSeed = 0;
    final int seedRate = 256;
    
    final int backTestRate = 487 * 13;
    final int backTestSampleSize = 13;
    
    MergePolicy mergePolicy;
    {
      MergePolicyBuilder builder = new MergePolicyBuilder();
      builder.setWriteAheadFlushTrigger(rowWidth * 1024);
      mergePolicy = builder.snapshot();
    }
    File rootDir = unitTestDir();
    
    // check available disk space..
    if (rootDir.getFreeSpace() / 20 < rowWidth * rowCount) {
      String message =
          "logical corpus size (" + rowWidth * rowCount +
          " bytes) > 5% of available disk space (" +
          new DecimalFormat("#,###").format(rootDir.getFreeSpace()) + " bytes)";
      log.error(message);
      fail(message);
    }
    
    TStoreConfig config = new Builder()
        .setRowWidth(rowWidth)
        .setDeleteCodec(deleteCodec)
        .setRowOrder(order)
        .setRootDir(rootDir)
        .setMergePolicy(mergePolicy)
        .toConfig();
    
    TStore tableStore = new TStore(config, true);
    
    
    ByteBuffer buffer = ByteBuffer.allocate(tableStore.getConfig().getRowWidth());
    RandomSequence testSeq = new RandomSequence(randomSeed, seedRate);
    long backTestPoint = backTestRate;
    
    DecimalFormat numFormatter = new DecimalFormat("#,###");
    
    for (long index = 0; index < rowCount; ++index ) {
      
      if (index == backTestPoint) {
        log.info("Back testing at sequence index: " + numFormatter.format(index));
        for (long incr = index / backTestSampleSize, ti = incr; ti < index; ti += incr) {
          testSeq.jumpTo(ti);
          buffer.clear();
          long key = testSeq.next();
          buffer.putLong(key).rewind();
          ByteBuffer row = tableStore.getRow(buffer);
          assertNotNull(row);
          assertNotSame(buffer, row);
          ByteBuffer expected = buffer;
          prepare64ByteRow(expected, key);
          assertEquals(expected, row);
        }
         // revert the test sequence to _index_
        testSeq.jumpTo(index);
        backTestPoint += backTestRate;
        log.info("Back testing passed.");
      }
      
      long key = testSeq.next();
      prepare64ByteRow(buffer, key);
      tableStore.setRow(buffer);
    }
    
    tableStore.close();
  }
  
  
  
  private void prepare64ByteRow(ByteBuffer row, long key) {
    row.clear();
    row.putLong(key);
    if (++key == CODEC_MAGIC)
      ++key;
    for (int countDown = 7; countDown-- > 0; )
      row.putLong(key++);
    row.flip();
  }
  
  
  private boolean checkEnabled() {
    boolean enabled = "true".equalsIgnoreCase(System.getProperty(RUN_PROPERTY));
    if (!enabled) {
      log.info("Skipping " + getMethod() + ". To activate -D" + RUN_PROPERTY + "=true");
    }
    return enabled;
  }

}
