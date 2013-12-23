/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.junit.Test;

import com.gnahraf.io.buffer.Covenant;
import com.gnahraf.io.store.karoon.TStoreConfig.Builder;
import com.gnahraf.io.store.karoon.merge.MergePolicy;
import com.gnahraf.io.store.karoon.merge.MergePolicyBuilder;
import com.gnahraf.io.store.table.SortedTableTest;
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
  public void test64ByteRow() throws Exception {
    initUnitTestDir(new Object() { });
    if (!checkEnabled())
      return;
    
    log.info("+----------------------------------------------");
    log.info("|");
    log.info("| " + getMethod() + " write-heavy stress test");
    log.info("|");
    log.info("+----------------------------------------------");
    
    final long startTime = System.currentTimeMillis();
    
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
      builder.setMaxMergeThreads(5);
      builder.setMergeThreadPriority(7);
      builder.setGenerationalFactor(4);
      builder.setWriteAheadFlushTrigger(64 * 1024);
      mergePolicy = builder.snapshot();
      log.info("Merge policy settings: " + mergePolicy);
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
    
    final int writeAllocSize = rowWidth * 512;
    ByteBuffer writeOnceBuffer = ByteBuffer.allocate(writeAllocSize);
    for (long index = 0; index < rowCount; ++index ) {
      
      if (!writeOnceBuffer.hasRemaining()) {
        
        writeOnceBuffer.flip();
        tableStore.setRows(writeOnceBuffer, Covenant.WONT_MOD);
        writeOnceBuffer = ByteBuffer.allocate(writeAllocSize);
        
        if (index > backTestPoint) {
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
            expected.flip();
            assertEquals(expected, row);
          }
           // revert the test sequence to _index_
          testSeq.jumpTo(index);
          backTestPoint += backTestRate;
          log.info("Back testing passed.");
        }
        
      }
      
      long key = testSeq.next();
      prepare64ByteRow(writeOnceBuffer, key);
    }
    

    if (writeOnceBuffer.position() > 0) {
      writeOnceBuffer.flip();
      tableStore.setRows(writeOnceBuffer, Covenant.WONT_MOD);
    }
    
    final long lap = System.currentTimeMillis();
    final long seconds = (lap - startTime) / 1000;

    log.info("+----------------------------------------------");
    log.info("|");
    log.info("| Row insertions completed");
    log.info("| rows: " + numFormatter.format(rowCount));
    log.info("| lap:  " + numFormatter.format(seconds) + " seconds");
    log.info("|");
    log.info("| rate: " + numFormatter.format(rowCount / seconds) + " rows/sec");
    log.info("|");
    log.info("+----------------------------------------------");
    log.info("Preparing to shutdown..");
    CommitRecord commitSnapshot = tableStore.getCurrentCommit();
    while (true) {
      int count = commitSnapshot.getTableIds().size();
      if (count < 32)
        break;
      log.info("waiting on merge.. table count is " + count);
      tableStore.waitForCommitChange(commitSnapshot.getId());
      commitSnapshot = tableStore.getCurrentCommit();
    }
    
    
    log.info("Shutting down..");
    
    tableStore.close();
    

    final long flap = System.currentTimeMillis();
    final long shutdownSeconds = (flap - lap) / 1000;
    final long amortizedSeconds = (flap - startTime) / 1000;

    log.info("+----------------------------------------------");
    log.info("|");
    log.info("| Shutdown completed");
    log.info("| Time taken to shutdown: " + numFormatter.format(shutdownSeconds) + " seconds");
    log.info("| rows: " + numFormatter.format(rowCount));
    log.info("| amortized time:  " + numFormatter.format(amortizedSeconds) + " seconds");
    log.info("| final table count: " + tableStore.getCurrentCommit().getTableIds().size());
    log.info("|");
    log.info("| amortized insertion rate: " + numFormatter.format(rowCount / amortizedSeconds) + " rows/sec");
    log.info("|");
    log.info("+----------------------------------------------");
  }
  
  
  
  private void prepare64ByteRow(ByteBuffer row, long key) {
//    row.clear();
    row.putLong(key);
    if (++key == CODEC_MAGIC)
      ++key;
    for (int countDown = 7; countDown-- > 0; )
      row.putLong(key++);
//    row.flip();
  }
  
  
  private boolean checkEnabled() {
    boolean enabled =
        "true".equalsIgnoreCase(System.getProperty(RUN_PROPERTY)) ||
        Boolean.valueOf(System.getProperty(SortedTableTest.PERF_TEST_PROPERTY)) ||
        getClass().getSimpleName().equals(System.getProperty("test"));
    if (!enabled) {
      log.info(
          "Skipping " + getMethod() +
          ". To activate -D" + RUN_PROPERTY + "=true or -D" +
          SortedTableTest.PERF_TEST_PROPERTY + "=true or -Dtest=" +
          getClass().getSimpleName());
    }
    return enabled;
  }

}
