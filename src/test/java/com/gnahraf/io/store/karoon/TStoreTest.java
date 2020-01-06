/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import com.gnahraf.test.TestMethodHarness;
import com.gnahraf.io.FileUtils;
import com.gnahraf.io.store.karoon.TStoreConfig.Builder;
import com.gnahraf.io.store.karoon.merge.MergePolicy;
import com.gnahraf.io.store.karoon.merge.MergePolicyBuilder;
import com.gnahraf.io.store.table.del.DeleteCodec;
import com.gnahraf.io.store.table.del.MagicNumDeleteCodec;
import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.io.store.table.order.RowOrders;

/**
 * 
 * @author Babak
 */
public class TStoreTest extends TestMethodHarness {
  
  static class AuditTStore extends TStore {
    
    public final static String TRASH_DIRNAME = "removed";
    private final File trashDir;

    public AuditTStore(TStoreConfig config, boolean create) throws IOException {
      super(config, create);
      this.trashDir = new File(config.getRootDir(), TRASH_DIRNAME);
      FileUtils.ensureDir(this.trashDir);
    }

    @Override
    protected void discardFile(File file) {
      
      if (file.exists()) {
        try {
          FileUtils.moveToDir(file, this.trashDir);
        } catch (FileNotFoundException fnfx) {
          LOG.warning(fnfx.getMessage());
        }
      }
    }
    
  }
  

  @Test
  public void testLoadNonexistent() throws IOException {
    initUnitTestDir(new Object() { });
    
    int rowWidth = 8;
    RowOrder order = RowOrders.INT_ORDER;
    DeleteCodec deleteCodec = MagicNumDeleteCodec.newIntInstance(4, 0);
    MergePolicy mergePolicy = new MergePolicyBuilder().snapshot();
    File rootDir = unitTestDir();
    
    TStoreConfig config = new Builder()
        .setRowWidth(rowWidth)
        .setDeleteCodec(deleteCodec)
        .setRowOrder(order)
        .setRootDir(rootDir)
        .setMergePolicy(mergePolicy)
        .toConfig();
    
    try {
      new TStore(config, false);
      fail();
    } catch (FileNotFoundException expected) {
      log.info("expected error: " + expected.getMessage());
    }
  }

  
  @Test
  public void testCreate() throws IOException {
    initUnitTestDir(new Object() { });
    
    int rowWidth = 8;
    RowOrder order = RowOrders.INT_ORDER;
    DeleteCodec deleteCodec = MagicNumDeleteCodec.newIntInstance(4, 0);
    MergePolicy mergePolicy = new MergePolicyBuilder().snapshot();
    File rootDir = unitTestDir();
    
    TStoreConfig config = new Builder()
        .setRowWidth(rowWidth)
        .setDeleteCodec(deleteCodec)
        .setRowOrder(order)
        .setRootDir(rootDir)
        .setMergePolicy(mergePolicy)
        .toConfig();
    
    TStore tableStore = new TStore(config, true);
    tableStore.close();
    tableStore = new TStore(config, false);
    tableStore.close();
    ByteBuffer key = ByteBuffer.allocate(rowWidth);
    ByteBuffer row = tableStore.getRow(key);
    assertNull(row);
  }

  
  @Test
  public void testCreate2() throws IOException {
    initUnitTestDir(new Object() { });
    
    int rowWidth = 8;
    RowOrder order = RowOrders.INT_ORDER;
    DeleteCodec deleteCodec = MagicNumDeleteCodec.newIntInstance(4, 0);
    MergePolicy mergePolicy = new MergePolicyBuilder().snapshot();
    File rootDir = unitTestDir();
    
    TStoreConfig config = new Builder()
        .setRowWidth(rowWidth)
        .setDeleteCodec(deleteCodec)
        .setRowOrder(order)
        .setRootDir(rootDir)
        .setMergePolicy(mergePolicy)
        .toConfig();
    
    TStore tableStore = new TStore(config, true);
    tableStore.close();
    tableStore = new TStore(config, true);
    tableStore.close();
    ByteBuffer key = ByteBuffer.allocate(rowWidth);
    ByteBuffer row = tableStore.getRow(key);
    assertNull(row);
  }

  
  @Test
  public void testOneRow() throws IOException {
    initUnitTestDir(new Object() { });
    
    int rowWidth = 8;
    RowOrder order = RowOrders.INT_ORDER;
    DeleteCodec deleteCodec = MagicNumDeleteCodec.newIntInstance(4, 0);
    MergePolicy mergePolicy = new MergePolicyBuilder().snapshot();
    File rootDir = unitTestDir();
    
    TStoreConfig config = new Builder()
        .setRowWidth(rowWidth)
        .setDeleteCodec(deleteCodec)
        .setRowOrder(order)
        .setRootDir(rootDir)
        .setMergePolicy(mergePolicy)
        .toConfig();
    
    TStore tableStore = new TStore(config, true);

    final int key = 4;
    final int value = 11;
    ByteBuffer row = ByteBuffer.allocate(rowWidth);
    row.putInt(key).putInt(value).rewind();
    tableStore.setRow(row);
    row.clear();
    row.putInt(key).putInt(0).rewind();
    ByteBuffer outRow = tableStore.getRow(row);
    assertEquals(key, outRow.getInt());
    assertEquals(value, outRow.getInt());
    tableStore.close();
    
    // reload and test
    tableStore = new TStore(config, false);
    outRow = tableStore.getRow(row);
    assertEquals(key, outRow.getInt());
    assertEquals(value, outRow.getInt());
    tableStore.close();
  }
  

  
  @Test
  public void testAFewRows() throws IOException {
    initUnitTestDir(new Object() { });
    
    int rowCount = 5;
    int keySpace = rowCount * rowCount;
    int valSpace = keySpace;
    
    int rowWidth = 8;
    RowOrder order = RowOrders.INT_ORDER;
    DeleteCodec deleteCodec = MagicNumDeleteCodec.newIntInstance(4, 0);
    MergePolicy mergePolicy = new MergePolicyBuilder().snapshot();
    File rootDir = unitTestDir();
    
    TStoreConfig config = new Builder()
        .setRowWidth(rowWidth)
        .setDeleteCodec(deleteCodec)
        .setRowOrder(order)
        .setRootDir(rootDir)
        .setMergePolicy(mergePolicy)
        .toConfig();
    
    TStore tableStore = new TStore(config, true);
    
    Random random = new Random(getMethod().hashCode());
    HashMap<Integer, Integer> expected = new HashMap<>();
    ByteBuffer rowBuffer = ByteBuffer.allocate(config.getRowWidth());
    
    for (int count = rowCount; count-- > 0;) {
      int key = random.nextInt(keySpace);
      int val = random.nextInt(valSpace) + 1;
      expected.put(key, val);
      rowBuffer.clear();
      rowBuffer.putInt(key).putInt(val).rewind();
      tableStore.setRow(rowBuffer);
    }
    
    assertContainsExpected(tableStore, expected);
    
    tableStore.close();
    tableStore = new TStore(config, false);

    assertContainsExpected(tableStore, expected);
    
    tableStore.close();
  }

  

  
  @Test
  public void testAFewRowsB() throws IOException {
    initUnitTestDir(new Object() { });
    
    int rowWidth = 8;
    RowOrder order = RowOrders.INT_ORDER;
    DeleteCodec deleteCodec = MagicNumDeleteCodec.newIntInstance(4, 0);
    MergePolicy mergePolicy = new MergePolicyBuilder().snapshot();
    File rootDir = unitTestDir();
    
    TStoreConfig config = new Builder()
        .setRowWidth(rowWidth)
        .setDeleteCodec(deleteCodec)
        .setRowOrder(order)
        .setRootDir(rootDir)
        .setMergePolicy(mergePolicy)
        .toConfig();
    
    TStore tableStore = new TStore(config, true);
    
    Random random = new Random(getMethod().hashCode());
    HashMap<Integer, Integer> expected = new HashMap<>();
    ByteBuffer rowBuffer = ByteBuffer.allocate(config.getRowWidth());
    
    int rowCount = 5;
    int valSpace = rowCount * rowCount;
    
    for (int count = rowCount; count-- > 0;) {
      int key = random.nextInt(rowCount);
      int val = random.nextInt(valSpace) + 1;
      expected.put(key, val);
      rowBuffer.clear();
      rowBuffer.putInt(key).putInt(val).rewind();
      tableStore.setRow(rowBuffer);
    }
    
    assertContainsExpected(tableStore, expected);
    
    tableStore.close();
    tableStore = new TStore(config, false);

    assertContainsExpected(tableStore, expected);
    
    tableStore.close();
  }
  
  @Test
  public void test1kRows() throws IOException {
    initUnitTestDir(new Object() { });
    
    int rowCount = 1000;
    int keySpace = rowCount * rowCount;
    int valSpace = keySpace;
    
    testSimpleImpl(rowCount, keySpace, valSpace);
  }
  
  @Test
  public void test2kRows() throws IOException {
    initUnitTestDir(new Object() { });
    
    int rowCount = 2000;
    int keySpace = rowCount * rowCount;
    int valSpace = keySpace;
    
    testSimpleImpl(rowCount, keySpace, valSpace);
  }
  
  @Test
  public void test3kRows() throws IOException {
    initUnitTestDir(new Object() { });
    
    int rowCount = 3000;
    int keySpace = rowCount * rowCount;
    int valSpace = keySpace;
    
    testSimpleImpl(rowCount, keySpace, valSpace);
  }
  
  
  
  private void testSimpleImpl(int rowCount, int keySpace, int valSpace) throws IOException {

    int rowWidth = 8;
    RowOrder order = RowOrders.INT_ORDER;
    DeleteCodec deleteCodec = MagicNumDeleteCodec.newIntInstance(4, 0);
    MergePolicy mergePolicy = new MergePolicyBuilder().snapshot();
    File rootDir = unitTestDir();
    
    TStoreConfig config = new Builder()
        .setRowWidth(rowWidth)
        .setDeleteCodec(deleteCodec)
        .setRowOrder(order)
        .setRootDir(rootDir)
        .setMergePolicy(mergePolicy)
        .toConfig();
    
    TStore tableStore = new TStore(config, true);
    
    Random random = new Random(getMethod().hashCode());
    HashMap<Integer, Integer> expected = new HashMap<>();
    ByteBuffer rowBuffer = ByteBuffer.allocate(config.getRowWidth());
    
    
    for (int count = rowCount; count-- > 0;) {
      int key = random.nextInt(keySpace);
      int val = random.nextInt(valSpace) + 1;
      expected.put(key, val);
      rowBuffer.clear();
      rowBuffer.putInt(key).putInt(val).rewind();
      tableStore.setRow(rowBuffer);
    }
    
    log.info("testing " + tableStore + " (" + expected.size() + " rows)");
    assertContainsExpected(tableStore, expected);
    
    // reload..
    tableStore.close();
    tableStore = new TStore(config, false);

    assertContainsExpected(tableStore, expected);
    
    tableStore.close();
  }
  
  
  
  

  public static void assertContainsExpected(TStore tableStore, Map<Integer, Integer> expected) throws IOException {
    
    ByteBuffer rowBuffer = ByteBuffer.allocate(tableStore.getConfig().getRowWidth());
    for (Map.Entry<Integer, Integer> entry : expected.entrySet()) {
      rowBuffer.clear();
      rowBuffer.putInt(entry.getKey()).rewind();
      ByteBuffer row = tableStore.getRow(rowBuffer);
      assertNotNull(row);
      assertEquals(entry.getKey().intValue(), row.getInt());
      assertEquals(entry.getValue().intValue(), row.getInt());
    }
  }
}
