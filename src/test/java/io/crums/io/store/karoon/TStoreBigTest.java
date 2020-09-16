/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.karoon;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import org.junit.Test;

import io.crums.io.buffer.Covenant;
import io.crums.io.store.karoon.CommitRecord;
import io.crums.io.store.karoon.TStore;
import io.crums.io.store.karoon.TStoreConfig;
import io.crums.io.store.karoon.TStoreConfig.Builder;
import io.crums.io.store.karoon.merge.MergePolicy;
import io.crums.io.store.karoon.merge.MergePolicyBuilder;
import io.crums.io.store.table.SortedTableTest;
import io.crums.io.store.table.del.DeleteCodec;
import io.crums.io.store.table.del.MagicNumDeleteCodec;
import io.crums.io.store.table.order.RowOrder;
import io.crums.io.store.table.order.RowOrders;
import io.crums.test.RandomSequence;
import io.crums.test.TestMethodHarness;

/**
 * 
 * @author Babak
 */
public class TStoreBigTest extends TestMethodHarness {
  
  public final static String RUN_PROPERTY = TStoreBigTest.class.getSimpleName();
  public final static String COUNT_PROPERTY = "count";
  public final static String ROW_WIDTH_PROPERTY = "rowWidth";
  public final static String SEED_PROPERTY = "seed";
  
  private final static RowOrder ORDER = RowOrders.LONG_ORDER;
  private final static long CODEC_MAGIC = -1;
  
  
  protected DeleteCodec deleteCodec = MagicNumDeleteCodec.newLongInstance(8, CODEC_MAGIC);

  
  private String propertyUsage(String property) {
    return ". To set use the -D" + property + "=... option.";
  }
  
  
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
    
    final int rowWidth;
    {
      String width = System.getProperty(ROW_WIDTH_PROPERTY);
      if (width == null)
        rowWidth = 64;
      else
        rowWidth = Integer.parseInt(width);
      log.info("Row byte width: " + rowWidth + propertyUsage(ROW_WIDTH_PROPERTY));
      if (rowWidth < 16 || rowWidth % 8 != 0)
        fail(ROW_WIDTH_PROPERTY + " must be a multiple of 8 and >= 16; actual was " + rowWidth);
    }
    final long rowCount;
    {
      String count = System.getProperty(COUNT_PROPERTY);
      if (count == null)
        rowCount = 1024 * 1024;
      else
        rowCount = Long.parseLong(count);
      log.info("Number of rows to be inserted: " + new DecimalFormat("#,###").format(rowCount) + propertyUsage(COUNT_PROPERTY));
    }
    final long randomSeed;
    {
      String seed = System.getProperty(SEED_PROPERTY);
      if (seed == null)
        randomSeed = System.currentTimeMillis();
      else
        randomSeed = Long.parseLong(seed);
      log.info("Random number generator seed: " + seed + propertyUsage(SEED_PROPERTY));
    }
    final int seedRate = 256;
    
    final int backTestSampleSize = 487 * 13;
    
    MergePolicy mergePolicy;
    {
      MergePolicyBuilder builder = new MergePolicyBuilder();
      builder.setWriteAheadFlushTrigger(rowWidth * 1024);
      builder.setMaxMergeThreads(8);
      builder.setMergeThreadPriority(3);
      builder.setGenerationalFactor(3);
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
      log.severe(message);
      fail(message);
    }
    
    TStoreConfig config = new Builder()
        .setRowWidth(rowWidth)
        .setDeleteCodec(deleteCodec)
        .setRowOrder(ORDER)
        .setRootDir(rootDir)
        .setMergePolicy(mergePolicy)
        .toConfig();
    
    TStore tableStore = new TStore(config, true);
    
    
    ByteBuffer buffer = ByteBuffer.allocate(tableStore.getConfig().getRowWidth());
    RandomSequence testSeq = new RandomSequence(randomSeed, seedRate);
    
    DecimalFormat numFormatter = new DecimalFormat("#,###");
    
    final int writeAllocSize = rowWidth * 512;
    ByteBuffer writeOnceBuffer = ByteBuffer.allocate(writeAllocSize);
    for (long index = 0; index < rowCount; ++index ) {
      
      if (!writeOnceBuffer.hasRemaining()) {
        
        writeOnceBuffer.flip();
        tableStore.setRows(writeOnceBuffer, Covenant.WONT_MOD);
        writeOnceBuffer = ByteBuffer.allocate(writeAllocSize);
        
      }
      
      long key = testSeq.next();
      prepareRow(writeOnceBuffer, key, rowWidth);
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
    
    log.info("");
    log.info("Back testing on reload..");
    log.info("");
    tableStore = new TStore(config, false);
    log.info("Reloaded table store " + tableStore);
    log.info("Back test sample size: " + backTestSampleSize);
    for (long incr = rowCount / backTestSampleSize, ti = 0; ti < rowCount; ti += incr) {
      testSeq.jumpTo(ti);
      buffer.clear();
      long key = testSeq.next();
      buffer.putLong(key).rewind();
      ByteBuffer row = tableStore.getRow(buffer);
      assertNotNull(row);
      assertNotSame(buffer, row);
      ByteBuffer expected = buffer;
      prepareRow(expected, key, rowWidth);
      expected.flip();
      assertEquals(expected, row);
    }
    
    log.info("Back testing passed.");
    tableStore.close();
  }
  
  
  
  private void prepareRow(ByteBuffer row, long key, int rowWidth) {
//    row.clear();
    row.putLong(key);
    if (++key == CODEC_MAGIC)
      ++key;
    for (int countDown = rowWidth / 8; --countDown > 0; )
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
