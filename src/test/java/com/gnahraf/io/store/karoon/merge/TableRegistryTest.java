/*
 * Copyright 2014 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import org.junit.Test;

import com.gnahraf.io.Releaseable;
import com.gnahraf.io.store.karoon.CommitRecord;
import com.gnahraf.test.TestMethodHarness;
import com.gnahraf.util.CollectionUtils;

/**
 * 
 * @author Babak
 */
public class TableRegistryTest extends TestMethodHarness {

  private LifecycleListener lifeListener = new LifecycleListener();
  private TableRegistry registry = new TableRegistry(lifeListener);
  
  class LifecycleListener implements TableLifecycleListener {
    final LinkedHashSet<Long> initedIds = new LinkedHashSet<>();
    final LinkedHashSet<Long> releasedIds = new LinkedHashSet<>();
    
    @Override
    public void inited(long tableId) {
      log.fine("lc: inited   " + tableId);
      assertTrue(initedIds.add(tableId));
    }

    @Override
    public void released(long tableId) {
      log.fine("lc: released " + tableId);
      assertTrue(releasedIds.add(tableId));
      assertTrue(initedIds.contains(tableId));
    }
    
  }

  @Test
  public void testAFewA() throws IOException {
    // boilerplate test dir setup
    initUnitTestDir(new Object() { });
    
    Long[] tableIds = { 89L, 4L, 30L, 52L, 21L, 64L, 850L, 854L };
    
    CommitRecord commit = newCommitRecord(tableIds, 1);
    
    int srcStartIndex = 1;
    int srcEndIndex = 3;
    
    Releaseable checkout = checkout(commit, srcStartIndex, srcEndIndex);
    assertNotNull(checkout);
    assertEquals(commit.getTableIds().size(), lifeListener.initedIds.size());
    assertTrue(lifeListener.initedIds.containsAll(commit.getTableIds()));
    assertTrue(lifeListener.releasedIds.isEmpty());
    
    checkout.close();
    assertTrue(lifeListener.releasedIds.isEmpty());
    assertEquals(commit.getTableIds().size(), lifeListener.initedIds.size());
  }

  
  
  @Test
  public void testAFewB() throws IOException {
    // boilerplate test dir setup
    initUnitTestDir(new Object() { });
    
    Long[] tableIds = { 89L, 4L, 30L, 52L, 21L, 64L, 850L, 854L };
    
    CommitRecord commit = newCommitRecord(tableIds, 1);
    
    checkout(commit, 1, 3).close();
    
    tableIds = new Long[] { 89L, 396L, 52L, 21L, 64L, 850L, 854L };
    commit = newCommitRecord(tableIds, 2);
    
    this.registry.advanceCommit(commit);
    assertEquals(3 - 1, lifeListener.releasedIds.size());
  }

  
  
  @Test
  public void testAFewC() throws IOException {
    // boilerplate test dir setup
    initUnitTestDir(new Object() { });
    
    Long[] tableIds = { 89L, 4L, 30L, 52L, 21L, 64L, 850L, 854L };
    
    HashSet<Long> allIds = new HashSet<>();
    
    
    CommitRecord commit = newCommitRecord(tableIds, 1);
    allIds.addAll(commit.getTableIds());
    
    checkout(commit, 1, 3).close();
    
    tableIds = new Long[] { 89L, 396L, 52L, 21L, 64L, 850L, 854L };
    commit = newCommitRecord(tableIds, 2);
    allIds.addAll(commit.getTableIds());
    
    this.registry.advanceCommit(commit);

    tableIds = new Long[] { 999L };
    commit = newCommitRecord(tableIds, 3);
//    allIds.addAll(commit.getTableIds());
    
    this.registry.advanceCommit(commit);
    assertEquals(allIds.size(), lifeListener.releasedIds.size());
    assertTrue(allIds.containsAll(lifeListener.releasedIds));
    assertEquals(allIds.size() + 1, lifeListener.initedIds.size());
    assertTrue(lifeListener.initedIds.containsAll(allIds));
    assertTrue(lifeListener.initedIds.contains(tableIds[0]));
  }
  

  @Test
  public void testAFewD() throws IOException {
    // boilerplate test dir setup
    initUnitTestDir(new Object() { });
    
    Long[] tableIds = { 89L, 4L, 30L, 52L, 21L, 64L, 850L, 854L };
    
    CommitRecord commit = newCommitRecord(tableIds, 1);
    
    checkout(commit, 1, 3);
    
    assertNull(checkout(commit, 2, 4));
    assertNull(checkout(commit, 0, 2));
    assertNull(checkout(commit, 1, 3));
  }
  

  @Test
  public void testAFewE() throws IOException {
    // boilerplate test dir setup
    initUnitTestDir(new Object() { });
    
    Long[] tableIds = { 89L, 4L, 30L, 52L, 21L, 64L, 850L, 854L };
    
    CommitRecord commit = newCommitRecord(tableIds, 1);
    
    @SuppressWarnings("unused")
    Releaseable checkout = checkout(commit, 1, 3);
    Releaseable checkout2 = checkout(commit, 3, 7);
    assertNotNull(checkout2);
    
    assertNull(checkout(commit, 2, 4));
    assertNull(checkout(commit, 0, 2));
    assertNull(checkout(commit, 1, 8));
  }
  

  @Test
  public void testAFewF() throws IOException {
    // boilerplate test dir setup
    initUnitTestDir(new Object() { });
    
    Long[] tableIds =    { 89L, 4L, 30L, 52L, 21L, 64L, 850L, 854L };
    CommitRecord staleCommit = newCommitRecord(tableIds, 1);

    tableIds = new Long[] { 89L, 396L, 52L, 21L, 64L, 850L, 854L };
    CommitRecord commit = newCommitRecord(tableIds, 2);
    
    checkout(commit, 1, 3);
    
    Releaseable checkout2 = checkout(staleCommit, 3, 7);
    assertNull(checkout2);
  }
  

  @Test
  public void testAFewG() throws IOException {
    // boilerplate test dir setup
    initUnitTestDir(new Object() { });
    
    Long[] tableIds =    { 89L, 4L, 30L, 52L, 21L, 64L, 850L, 854L };
    CommitRecord commit = newCommitRecord(tableIds, 1);

    Releaseable checkout = checkout(commit, 4, 6);
    Releaseable checkout2 = checkout(commit, 1, 4);
    assertNotNull(checkout2);

    tableIds = new Long[] { 89L, 4L, 30L, 52L, 866L, 850L, 854L, 860L, 861L};
    commit = newCommitRecord(tableIds, 2);
    assertNull(checkout(commit, 0, 3));
    
    checkout.close();
    assertTrue(lifeListener.releasedIds.contains(21L));
    assertTrue(lifeListener.releasedIds.contains(64L));
    assertEquals(2, lifeListener.releasedIds.size());

    assertNull(checkout(commit, 0, 3));
    checkout2.close();
    
    Releaseable checkout3 = checkout(commit, 0, 3);
    assertNotNull(checkout3);
  }
  
  
  
  
  private Releaseable checkout(CommitRecord commit, int srcStartIndex, int srcEndIndex) {
    
    List<Long> backSetTableIds = commit.getTableIds().subList(0, srcStartIndex);
    List<Long> sourceTableIds = commit.getTableIds().subList(srcStartIndex, srcEndIndex);
    
    return registry.checkOut(sourceTableIds, backSetTableIds, commit);
  }
  
  
  
  private CommitRecord newCommitRecord(Long[] tids, long commitId) throws IOException {
    return newCommitRecord(CollectionUtils.asReadOnlyList(tids), commitId);
  }
  
  
  private CommitRecord newCommitRecord(List<Long> tids, long commitId) throws IOException {
    File file = new File(unitTestDir(), "C" + commitId + ".cmmt");
    return CommitRecord.create(file, tids, commitId);
  }

}
