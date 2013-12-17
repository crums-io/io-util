/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;

import static org.junit.Assert.*;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * 
 * @author Babak
 */
public class GenerationInfoTest {
  
  private static Logger LOG = Logger.getLogger(GenerationInfoTest.class);
  
  private final static MergePolicy MERGE_POLICY = new MergePolicyBuilder().snapshot();
  
  private final static long U = MERGE_POLICY.getWriteAheadFlushTrigger();
  
  private final static int F = MERGE_POLICY.getGenerationalFactor();
  

  @Test
  public void testEmpty() {
    List<TableInfo> tableStack = Collections.emptyList();
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertTrue(mergeCandidates.isEmpty());
    LOG.info(MERGE_POLICY);
  }

  @Test
  public void testEmpty2() {
    List<TableInfo> tableStack = Collections.emptyList();
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.generationalMergeCandidates(tableStack, MERGE_POLICY);
    assertTrue(mergeCandidates.isEmpty());
  }

  @Test
  public void testSingleFresh() {
    long[] ids =  { 5 };
    long[] sizes = { U };
    
    List<TableInfo> tableStack = generateTableStack(ids, sizes);
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertTrue(mergeCandidates.isEmpty());
  }

  @Test
  public void testDoubleFresh() {
    long[] ids =   { 5, 6 };
    long[] sizes = { U, U };
    
    List<TableInfo> tableStack = generateTableStack(ids, sizes);
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertEquals(1, mergeCandidates.size());
    GenerationInfo g = mergeCandidates.get(0);
    assertEquals(0, g.generation);
    assertEquals(tableStack, g.srcInfos);
    assertTrue(g.backSetInfos.isEmpty());
  }

  @Test
  public void testTripleFresh() {
    long[] ids =   { 5, 6, 7 };
    long[] sizes = { U, U - 1, 2 * U - 1 };
    
    List<TableInfo> tableStack = generateTableStack(ids, sizes);
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertEquals(1, mergeCandidates.size());
    GenerationInfo g = mergeCandidates.get(0);
    assertEquals(0, g.generation);
    assertEquals(tableStack, g.srcInfos);
    assertTrue(g.backSetInfos.isEmpty());
  }

  @Test
  public void testTripleFreshWithBackSet() {
    long[] ids =   { 1,   15, 16,  17 };
    long[] sizes = { U*U, U,  U-1, 2*U-1 };
    
    List<TableInfo> tableStack = generateTableStack(ids, sizes);
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertEquals(1, mergeCandidates.size());
    GenerationInfo g = mergeCandidates.get(0);
    assertEquals(0, g.generation);
    assertEquals(tableStack.subList(1, ids.length), g.srcInfos);
    assertEquals(tableStack.subList(0, 1), g.backSetInfos);
  }

  @Test
  public void testTripleFreshWithBackSet2() {
    long[] ids =   { 1,   2,   15, 16,  17 };
    long[] sizes = { U*U, U*U, U,  U-1, 2*U-1 };
    
    List<TableInfo> tableStack = generateTableStack(ids, sizes);
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertEquals(2, mergeCandidates.size());
    GenerationInfo g = mergeCandidates.get(0);
    assertEquals(0, g.generation);
    assertEquals(tableStack.subList(2, ids.length), g.srcInfos);
    assertEquals(tableStack.subList(0, 2), g.backSetInfos);
    GenerationInfo g1 = mergeCandidates.get(1);
    assertEquals(g.backSetInfos, g1.srcInfos);
    LOG.info("g1.generation: " + g1.generation);
    DecimalFormat f = new DecimalFormat("#,###");
    LOG.info("g1.generation min size: " + f.format(MERGE_POLICY.getGenerationMaxSize(g1.generation - 1)));
    LOG.info("g1.generation max size: " + f.format(MERGE_POLICY.getGenerationMaxSize(g1.generation)));
  }
  
  
  @Test
  public void testTwoOldOneFresh() {
    long[] ids =   { 1,   2,   15, };
    long[] sizes = { U*U, U*U, U,  };
    
    List<TableInfo> tableStack = generateTableStack(ids, sizes);
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertEquals(1, mergeCandidates.size());
    GenerationInfo g = mergeCandidates.get(0);
    assertTrue(g.generation > 0);
    assertEquals(tableStack.subList(0, 2), g.srcInfos);
    assertTrue(g.backSetInfos.isEmpty());
  }
  
  
  @Test
  public void testThreeOldOneFresh() {
    long[] ids =   { 1,   2,     15,  99, };
    long[] sizes = { U*U, F*U+1, U*U, U   };
    
    List<TableInfo> tableStack = generateTableStack(ids, sizes);
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertEquals(1, mergeCandidates.size());
    GenerationInfo g = mergeCandidates.get(0);
    assertTrue(g.generation > 0);
    assertEquals(tableStack.subList(0, 3), g.srcInfos);
    assertTrue(g.backSetInfos.isEmpty());
  }


  @Test
  public void testMany() {
    long[] ids = {
        27,
        32,
        11,
        53,
        121,
        44,
        62,
        239,
        345,
        718,
    };
    long[] sizes = {
        U*F*F*F*F + 1,
        U*F*F*F*F + 6,
        U*F*F*F + 1,
        U*F*F*F + 1,
        U*F*F + 1,
        U*F*F + 1,
        U*F + 1,
        U*F + 1,
        U,
        U-1,
    };
    List<TableInfo> tableStack = generateTableStack(ids, sizes);
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertEquals(5, mergeCandidates.size());
    for (int i = 0; i < mergeCandidates.size(); ++i) {
      GenerationInfo g = mergeCandidates.get(i);
      assertEquals(i, g.generation);
      int expectedGenStartIndex = ids.length - 2 * i - 2;
      assertEquals(
          tableStack.subList(expectedGenStartIndex, expectedGenStartIndex + 2),
          g.srcInfos);
      assertEquals(
          tableStack.subList(0, expectedGenStartIndex),
          g.backSetInfos);
    }
  }


  @Test
  public void testManyWithGap() {
    long[] ids = {
        27,
        32,
        11,
        53,
        121,
        44,
        345,
        718,
    };
    long[] sizes = {
        U*F*F*F*F + 1,
        U*F*F*F*F + 6,
        U*F*F*F + 1,
        U*F*F*F + 1,
        U*F*F + 1,
        U*F*F + 1,
        U,
        U-1,
    };
    List<TableInfo> tableStack = generateTableStack(ids, sizes);
    List<GenerationInfo> mergeCandidates =
        GenerationInfo.candidateMerges(tableStack, MERGE_POLICY, 0);
    assertEquals(4, mergeCandidates.size());
    for (int i = 1; i < mergeCandidates.size(); ++i) {
      GenerationInfo g = mergeCandidates.get(i);
      assertEquals(i + 1, g.generation);
      int expectedGenStartIndex = ids.length - 2 * i - 2;
      assertEquals(
          tableStack.subList(expectedGenStartIndex, expectedGenStartIndex + 2),
          g.srcInfos);
      assertEquals(
          tableStack.subList(0, expectedGenStartIndex),
          g.backSetInfos);
    }
  }
  
  
  
  private List<TableInfo> generateTableStack(long[] ids, long[] sizes) {
    ArrayList<TableInfo> stack = new ArrayList<>(ids.length);
    for (int i = 0; i < ids.length; ++i)
      stack.add(new TableInfo(ids[i], sizes[i]));
    return Collections.unmodifiableList(stack);
  }

}
