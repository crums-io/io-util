/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.karoon;


import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.gnahraf.test.IoTestCase;

import io.crums.util.CollectionUtils;

/**
 * 
 * @author Babak
 */
public class CommitRecordTest extends IoTestCase {

  @Test
  public void testEmpty() throws IOException {
    File recordFile = getMethodOutputFilepath(new Object() { });
    assertTrue(recordFile.createNewFile());
    CommitRecord record = CommitRecord.load(recordFile, 0);
    assertEquals(0, record.getTableIds().size());
    assertEquals(recordFile, record.getFile());
    assertEquals(0, record.getId());
  }

  @Test
  public void testEmpty2() throws IOException {
    File recordFile = getMethodOutputFilepath(new Object() { });;
    List<Long> tableIds = Collections.emptyList();
    CommitRecord.write(recordFile, tableIds);
    
    CommitRecord record = CommitRecord.load(recordFile, 0);
    assertEquals(0, record.getTableIds().size());
    assertEquals(recordFile, record.getFile());
  }
  
  @Test
  public void testOneId() throws IOException {
    File recordFile = getMethodOutputFilepath(new Object() { });
    List<Long> tableIds;
    {
      Long[] values = { 1L };
      tableIds = CollectionUtils.asReadOnlyList(values);
    }
    CommitRecord.write(recordFile, tableIds);
    
    CommitRecord record = CommitRecord.load(recordFile, 0);
    assertEquals(tableIds, record.getTableIds());
  }
  
  @Test
  public void testTwoIds() throws IOException {
    File recordFile = getMethodOutputFilepath(new Object() { });
    List<Long> tableIds;
    {
      Long[] values = { 1L, 5L};
      tableIds = CollectionUtils.asReadOnlyList(values);
    }
    CommitRecord.write(recordFile, tableIds);
    
    CommitRecord record = CommitRecord.load(recordFile, 0);
    assertEquals(tableIds, record.getTableIds());
  }
  
  @Test
  public void testManyIds() throws IOException {
    File recordFile = getMethodOutputFilepath(new Object() { });
    List<Long> tableIds;
    {
      Long[] values = { 89L, 4L, 30L, 52L, 21L, 64L, 850L, 573L };
      tableIds = CollectionUtils.asReadOnlyList(values);
    }
    CommitRecord.write(recordFile, tableIds);
    
    CommitRecord record = CommitRecord.load(recordFile, 0);
    assertEquals(tableIds, record.getTableIds());
  }
  

  
  @Test
  public void testDupIds() throws IOException {
    File recordFile = getMethodOutputFilepath(new Object() { });
    List<Long> tableIds;
    {
      Long[] values = { 89L, 4L, 30L, 52L, 21L, 64L, 850L, 4L };
      tableIds = CollectionUtils.asReadOnlyList(values);
    }
    try {
      CommitRecord.write(recordFile, tableIds);
      fail();
    } catch (Exception expected) {
      
    }
    assertFalse(recordFile.exists());
  }
  

  
  @Test
  public void testLeniencyWithEmpty() throws IOException {
    File recordFile = getMethodOutputFilepath(new Object() { });
    List<Long> tableIds = Collections.emptyList();
    try (FileWriter writer = new FileWriter(recordFile)) {
      writer.append("  \n\t \n ");
    }
    CommitRecord record = CommitRecord.load(recordFile, 0);
    assertEquals(tableIds, record.getTableIds());
  }
  

  
  @Test
  public void testLeniencyWithSome() throws IOException {
    File recordFile = getMethodOutputFilepath(new Object() { });
    List<Long> tableIds;
    {
      Long[] values = { 89L, 4L, 30L, 52L, 21L };
      tableIds = CollectionUtils.asReadOnlyList(values);
    }
    try (FileWriter writer = new FileWriter(recordFile)) {
      writer.append("  89 4 30\n\t 52 21 ");
    }
    CommitRecord record = CommitRecord.load(recordFile, 0);
    assertEquals(tableIds, record.getTableIds());
  }
  

}
