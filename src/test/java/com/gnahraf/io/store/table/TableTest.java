/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;


import static org.junit.Assert.*;
import static com.gnahraf.test.TestHelper.method;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import com.gnahraf.io.channels.ChannelUtils;
import com.gnahraf.io.store.ks.Keystone;
import com.gnahraf.io.store.ks.KeystoneImpl;
import com.gnahraf.io.store.ks.VolatileKeystone;
import com.gnahraf.io.store.table.Table;
import com.gnahraf.test.TestDirs;


public class TableTest {

  private final Logger LOG = Logger.getLogger(getClass());

  private FileChannel file;


  private void setup(String method) throws IOException {
    setup(method, false);
  }


  private void setup(String method, boolean exists) throws IOException {
    file = openFile(method, exists);
  }
  
  
  private FileChannel openFile(String method, boolean exists) throws IOException {
    File testFile = new File(TestDirs.getTestDir(getClass()), method);
    if (testFile.exists() != exists)
      throw new IllegalStateException("test file already exists: " + testFile);
    return new RandomAccessFile(testFile, "rw").getChannel();
  }


  @After
  public void tearDown() throws Exception {
    if (file != null)
      file.close();
  }


  @Test
  public void testContructorWithBadArgs() throws IOException {
    // boiler plate file setup..
    setup(method(new Object() { }));

    final int rowSize = 32;
    Keystone rowCount = new KeystoneImpl(file, 0, 0L);
    try {
      new Table(rowCount, file, -1, rowSize);
    } catch (Exception x) {
      LOG.info("expected error: " + x.getMessage());
    }
  }


  @Test
  public void testContructorWithBadArgs2() throws IOException {
    // boiler plate file setup..
    setup(method(new Object() { }));

    final int rowSize = 32;
    Keystone rowCount = new KeystoneImpl(file, 0, -1L);
    try {
      new Table(rowCount, file, rowCount.size(), rowSize);
    } catch (Exception x) {
      LOG.info("expected error: " + x.getMessage());
    }
  }


  @Test
  public void testContructorWithBadArgs3() throws IOException {
    // boiler plate file setup..
    setup(method(new Object() { }));

    final int rowSize = 32;
    Keystone rowCount = new KeystoneImpl(file, 0, 1L);
    try {
      new Table(rowCount, file, rowCount.size(), rowSize);
    } catch (Exception x) {
      LOG.info("expected error: " + x.getMessage());
    }
  }


  @Test
  public void testContructorWithBadArgs4() throws IOException {
    // boiler plate file setup..
    setup(method(new Object() { }));

    final int rowSize = 0;
    Keystone rowCount = new KeystoneImpl(file, 0, 0L);
    try {
      new Table(rowCount, file, rowCount.size(), rowSize);
    } catch (Exception x) {
      LOG.info("expected error: " + x.getMessage());
    }
  }


  @Test
  public void testEmpty() throws IOException {
    // boiler plate file setup..
    setup(method(new Object() { }));

    final int rowSize = 32;
    Keystone rowCount = new KeystoneImpl(file, 0, 0L);
    Table table = new Table(rowCount, file, rowCount.size(), rowSize);
    assertEquals(0, table.getRowCount());
    assertEquals(rowSize, table.getRowWidth());
  }


  @Test
  public void testEmpty2() throws IOException {
    // boiler plate file setup..
    setup(method(new Object() { }));

    final int rowSize = 32;
    byte[] header = "# Gnahraf -- Your header here -- v1.0\n".getBytes();
    ChannelUtils.writeRemaining(file, ByteBuffer.wrap(header));
    Table table = Table.newEmptyInstance(file, rowSize);
    assertEquals(0, table.getRowCount());
    assertEquals(rowSize, table.getRowWidth());
  }


  @Test
  public void testWithOneRow() throws IOException {
    // boiler plate file setup..
    setup(method(new Object() { }));

    final int rowSize = 32;
    Keystone rowCount = new KeystoneImpl(file, 0, 0L);
    Table table = new Table(rowCount, file, rowCount.size(), rowSize);
    ByteBuffer buffer = ByteBuffer.allocate(rowSize);
    buffer.clear();
    long[] row0 = new long[] { 1, -2, 3, -4 };
    for (int i = 0; i < row0.length; ++i)
      buffer.putLong(row0[i]);
    buffer.flip();
    table.append(buffer);
    assertEquals(1, table.getRowCount());

    ByteBuffer row = ByteBuffer.allocate(rowSize);
    row.clear();
    table.read(0, row);
    row.flip();

    buffer.clear();

    assertEquals(buffer, row);
  }


  @Test
  public void testWithOneRowVolatile() throws IOException {
    // boiler plate file setup..
    setup(method(new Object() { }));

    final int rowSize = 32;
    Keystone rowCount = new VolatileKeystone(0);
    Table table = new Table(rowCount, file, rowCount.size(), rowSize);
    ByteBuffer buffer = ByteBuffer.allocate(rowSize);
    buffer.clear();
    long[] row0 = new long[] { 1, -2, 3, -4 };
    for (int i = 0; i < row0.length; ++i)
      buffer.putLong(row0[i]);
    buffer.flip();
    table.append(buffer);
    assertEquals(1, table.getRowCount());

    ByteBuffer row = ByteBuffer.allocate(rowSize);
    row.clear();
    table.read(0, row);
    row.flip();

    buffer.clear();

    assertEquals(buffer, row);
  }


  @Test
  public void testWithOneRowVolatile2() throws IOException {
    // boiler plate file setup..
    setup(method(new Object() { }));

    final int rowSize = 32;
    Table table = Table.newSansKeystoneInstance(file, rowSize);
    ByteBuffer buffer = ByteBuffer.allocate(rowSize);
    buffer.clear();
    long[] row0 = new long[] { 1, -2, 3, -4 };
    for (int i = 0; i < row0.length; ++i)
      buffer.putLong(row0[i]);
    buffer.flip();
    table.append(buffer);
    assertEquals(1, table.getRowCount());

    ByteBuffer row = ByteBuffer.allocate(rowSize);
    row.clear();
    table.read(0, row);
    row.flip();

    buffer.clear();

    assertEquals(buffer, row);
  }


  @Test
  public void testWithOneRowWithReload() throws IOException {
    // boiler plate file setup..
    String method = method(new Object() { });
    setup(method);

    final int rowSize = 32;
    Keystone rowCount = new KeystoneImpl(file, 0, 0L);
    Table table = new Table(rowCount, file, rowCount.size(), rowSize);
    ByteBuffer buffer = ByteBuffer.allocate(rowSize);
    buffer.clear();
    long[] row0 = new long[] { 1, -2, 3, -4 };
    for (int i = 0; i < row0.length; ++i)
      buffer.putLong(row0[i]);
    buffer.flip();
    table.append(buffer);
    table.close();
    setup(method, true);
    table = Table.loadInstance(file, rowSize);
    assertEquals(1, table.getRowCount());

    ByteBuffer row = ByteBuffer.allocate(rowSize);
    row.clear();
    table.read(0, row);
    row.flip();

    buffer.clear();

    assertEquals(buffer, row);
  }


  @Test
  public void testTwoRowsReload() throws IOException {
    // boiler plate file setup..
    String method = method(new Object() { });
    setup(method);

    final int rowSize = 32;
    Keystone rowCount = new KeystoneImpl(file, 0, 0L);
    Table table = new Table(rowCount, file, rowCount.size(), rowSize);
    ByteBuffer buffer = ByteBuffer.allocate(2 * rowSize);
    buffer.clear();
    long[] row0 = new long[] { 1, -2, 3, -4 };
    long[] row1 = new long[] { 5, -6, 7, -8 };
    for (int i = 0; i < row0.length; ++i)
      buffer.putLong(row0[i]);
    for (int i = 0; i < row0.length; ++i)
      buffer.putLong(row1[i]);
    
    buffer.flip();
    table.append(buffer);
    table.close();
    setup(method, true);
    table = Table.loadInstance(file, rowSize);
    assertEquals(2, table.getRowCount());

    // test reading both rows
    ByteBuffer row = ByteBuffer.allocate(2 * rowSize);
    row.clear();
    table.read(0, row);
    row.flip();

    buffer.clear();

    assertEquals(buffer, row);
    
    // test reading row 1 alone
    row.clear().limit(rowSize);
    table.read(1, row);
    row.flip();
    buffer.clear().position(rowSize);
    
    
    assertEquals(buffer, row);
    
    // test reading row 0 alone
    row.clear().limit(rowSize);
    table.read(0, row);
    row.flip();
    buffer.clear().limit(rowSize);
    
    
    assertEquals(buffer, row);
  }


  @Test
  public void testTwoRowsReloadWithVolatileKeystone() throws IOException {
    // boiler plate file setup..
    String method = method(new Object() { });
    setup(method);

    final int rowSize = 24;
    
    long[][] rows = {
        { 1, -2, 3 },
        { 2, -3, 404 },
    };
    
    Table table = Table.newSansKeystoneInstance(file, rowSize);
    ByteBuffer buffer = ByteBuffer.allocate(rows.length * rowSize);
    buffer.clear();
    for (int i = 0; i < rows.length; ++i)
      for (int j = 0; j < 3; ++j)
        buffer.putLong(rows[i][j]);
    
    buffer.flip();
    table.append(buffer);
    
    // reload
    table.close();
    setup(method, true);
    table = Table.newSansKeystoneInstance(file, rowSize);
    assertEquals(rows.length, table.getRowCount());

    // test reading both rows
    ByteBuffer row = ByteBuffer.allocate(2 * rowSize);
    row.clear();
    table.read(0, row);
    row.flip();

    buffer.clear();

    assertEquals(buffer, row);
    
    // test reading row 1 alone
    row.clear().limit(rowSize);
    table.read(1, row);
    row.flip();
    buffer.clear().position(rowSize);
    
    
    assertEquals(buffer, row);
    
    // test reading row 0 alone
    row.clear().limit(rowSize);
    table.read(0, row);
    row.flip();
    buffer.clear().limit(rowSize);
    
    
    assertEquals(buffer, row);
  }
  

  @Test
  public void testAppendRows1() throws IOException {
    // boiler plate file setup..
    String method = method(new Object() { });
    setup(method);

    final int rowSize = 24;
    
    long[][] rows = {
        { 1, -2, 3 },
        { 2, -3, 404 },
        { 7,  3, 4 },
        { -5, 5, 987 },
        { 8, -3, 4 },
        { 77, -3, 4 },
        { 13, 0, 0 },
        { 34, 0, 44567 },
        { 39, 1, 0 },
        { 70, 12, 89 },
        { 46, 3, 613 },
        { 99, 83, 4 },
        { 989, 30, 88 },
        { 45, -389, 76 },
        { 33, -30, 60 },
        { 22, -32, 111 },
        { 11, 53, 41 },
        { 67, 567, 4333 },
        { 901, -64, 24 },
        { 82, 345, 43 },
        { 8, 128, 14 },
//        { 909, -256, 4555 },
    };
    
    Table table = Table.newSansKeystoneInstance(file, rowSize);
    ByteBuffer buffer = ByteBuffer.allocate(rows.length * rowSize);
    buffer.clear();
    for (int i = 0; i < rows.length; ++i)
      for (int j = 0; j < 3; ++j)
        buffer.putLong(rows[i][j]);
    
    buffer.flip();
    table.append(buffer);
    
    // reload
    table.close();
    setup(method, true);
    table = Table.newSansKeystoneInstance(file, rowSize);
    assertEquals(rows.length, table.getRowCount());
    
    String targetFilename = method + "-target";
    Table target = Table.newSansKeystoneInstance(
        openFile(targetFilename, false),
        rowSize);
    
    // cut the table at the half point..
    final int pivot = rows.length / 2;
    
    target.appendRows(table, pivot, table.getRowCount() - pivot);
    assertEquals(table.getRowCount() - pivot, target.getRowCount());
    
    target.appendRows(table, 0, pivot);
    target.close();
    
    // reload..
    target = Table.newSansKeystoneInstance(
        openFile(targetFilename, true),
        rowSize);
    assertEquals(table.getRowCount(), target.getRowCount());
    

    ByteBuffer buffer2 = ByteBuffer.allocate(rows.length * rowSize);
    buffer2.clear().limit((rows.length - pivot) * rowSize);
    
    target.read(0, buffer2);
    buffer2.flip();
    
    buffer.clear().position(pivot * rowSize);
    
    assertEquals(buffer, buffer2);
    
    buffer2.clear().limit(pivot * rowSize);
    target.read(rows.length - pivot, buffer2);
    buffer2.flip();
    
    buffer.clear().limit(pivot * rowSize);
    
    assertEquals(buffer, buffer2);
    
    target.close();
  }
  

}

