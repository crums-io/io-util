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
  
  private final static double F = MERGE_POLICY.getGenerationalFactor();
  

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
    long[] sizes = { U*U, (long) (F*U+1), U*U, U   };
    
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
        (long) (U*F*F*F*F + 1),
        (long) (U*F*F*F*F + 6),
        (long) (U*F*F*F + 1),
        (long) (U*F*F*F + 1),
        (long) ( U*F*F + 1),
        (long) (U*F*F + 1),
        (long) (U*F + 1),
        (long) (U*F + 1),
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
      
      GenerationInfo gd = GenerationInfo.candidateMerge(tableStack, MERGE_POLICY, i);
      assertEquals(g, gd);
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
        (long) (U*F*F*F*F + 1),
        (long) (U*F*F*F*F + 6),
        (long) (U*F*F*F + 1),
        (long) (U*F*F*F + 1),
        (long) (U*F*F + 1),
        (long) (U*F*F + 1),
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
  
  
  
  @Test
  public void testReduceBy() {
    long[] preIds = {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        27,
        32,
    };
    long[] preSizes = {
        (long) (U*F*F*F*F + 1),
        (long) (U*F*F*F*F + 6),
        (long) (U*F*F*F + 1),
        (long) (U*F*F*F + 1),
        (long) (U*F*F + 1),
        (long) (U*F*F + 1),
        U,
        U,
        U,
    };
    
    
    long[] postIds = {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        27,
        32,
        11,
        53,
        121,
        44,
        345,
    };
    long[] postSizes = {
        (long) (U*F*F*F*F + 1),
        (long) (U*F*F*F*F + 6),
        (long) (U*F*F*F + 1),
        (long) (U*F*F*F + 1),
        (long) (U*F*F + 1),
        (long) (U*F*F + 1),
        U,
        U,
        U,
        U,
        U,
        U,
        U,
        U-1,
    };
    
    List<TableInfo> preStack = generateTableStack(preIds, preSizes);
    List<TableInfo> postStack = generateTableStack(postIds, postSizes);
    
    GenerationInfo young0 = GenerationInfo.candidateMerge(preStack, MERGE_POLICY, 0);
    GenerationInfo young1 = GenerationInfo.candidateMerge(postStack, MERGE_POLICY, 0);
    GenerationInfo yReduced = young1.reduceBy(young0);
    
    assertEquals(
        young1.srcInfos.size() - young0.srcInfos.size(),
        yReduced.srcInfos.size());
    
    Object expected =
        young1.srcInfos.subList(young0.srcInfos.size(), young1.srcInfos.size());
    
    assertEquals(expected, yReduced.srcInfos);
    
    assertEquals(
        young0.backSetInfos.size() + young0.srcInfos.size(),
        yReduced.backSetInfos.size());
    
    assertEquals(young0.backSetInfos, young1.backSetInfos);
    assertEquals(
        young0.backSetInfos,
        yReduced.backSetInfos.subList(0, young0.backSetInfos.size()));
    assertEquals(
        young0.srcInfos,
        yReduced.backSetInfos.subList(
            young0.backSetInfos.size(),
            yReduced.backSetInfos.size()));
  }
  
  
  
  @Test
  public void testReduceBy2() {
    long[] preIds = {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        27,
        32,
    };
    long[] preSizes = {
        (long) (U*F*F*F*F + 1),
        (long) (U*F*F*F*F + 6),
        (long) (U*F*F*F + 1),
        (long) (U*F*F*F + 1),
        (long) (U*F*F + 1),
        (long) (U*F*F + 1),
        U,
        U,
        U,
    };
    
    
    long[] postIds = {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        27,
        32,
        11,
        53,
        121,
        44,
        345,
    };
    long[] postSizes = {
        (long) (U*F*F*F*F + 1),
        (long) (U*F*F*F*F + 6),
        (long) (U*F*F*F + 1),
        (long) (U*F*F*F + 1),
        (long) (U*F*F + 1),
        (long) (U*F*F + 1),
        U,
        U,
        U,
        U,
        U,
        U,
        U,
        U-1,
    };
    
    List<TableInfo> preStack = generateTableStack(preIds, preSizes);
    List<TableInfo> postStack = generateTableStack(postIds, postSizes);
    
    GenerationInfo young0 = GenerationInfo.candidateMerge(preStack, MERGE_POLICY, 0);
    GenerationInfo young1 = GenerationInfo.candidateMerge(postStack, MERGE_POLICY, 0);
    GenerationInfo yReduced1 = young1.reduceBy(young0);
    
    List<TableInfo> stack = new ArrayList<>(postStack);
    long nextId = 1000;
    
    final int count = 3;
    for (int countDown = count; countDown-- > 0;)
      stack.add(new TableInfo(nextId++, U));
    
    GenerationInfo young2 = GenerationInfo.candidateMerge(stack, MERGE_POLICY, 0);
    GenerationInfo yReduced2 = young2.reduceBy(yReduced1);
    
    assertEquals(count, yReduced2.srcInfos.size());

    Object expected = stack.subList(stack.size() - count, stack.size());
    assertEquals(expected, yReduced2.srcInfos);
    
    assertEquals(
        yReduced1.backSetInfos,
        yReduced2.backSetInfos.subList(0, yReduced1.backSetInfos.size()));
    
    assertEquals(
        yReduced1.srcInfos,
        yReduced2.backSetInfos.subList(
            yReduced1.backSetInfos.size(),
            yReduced2.backSetInfos.size()));
  }
  
  
  @Test
  public void testReduceBy3() {
    long[] preIds = {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        27,
        32,
    };
    long[] preSizes = {
        (long) (U*F*F*F*F + 1),
        (long) (U*F*F*F*F + 6),
        (long) (U*F*F*F + 1),
        (long) (U*F*F*F + 1),
        (long) (U*F*F + 1),
        (long) (U*F*F + 1),
        U,
        U,
        U,
    };
    
    
    long[] postIds = {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        27,
        32,
        11,
        53,
        121,
        44,
        345,
    };
    long[] postSizes = {
        (long) (U*F*F*F*F + 1),
        (long) (U*F*F*F*F + 6),
        (long) (U*F*F*F + 1),
        (long) (U*F*F*F + 1),
        (long) (U*F*F + 1),
        (long) (U*F*F + 1),
        U,
        U,
        U,
        U,
        U,
        U,
        U,
        U-1,
    };
    
    List<TableInfo> preStack = generateTableStack(preIds, preSizes);
    List<TableInfo> postStack = generateTableStack(postIds, postSizes);
    
    GenerationInfo young0 = GenerationInfo.candidateMerge(preStack, MERGE_POLICY, 0);
    GenerationInfo young1 = GenerationInfo.candidateMerge(postStack, MERGE_POLICY, 0);
    GenerationInfo yReduced1 = young1.reduceBy(young0);
    
    List<TableInfo> stack = new ArrayList<>(postStack);
    long nextId = 1000;
    
    final int count = 2;
    for (int countDown = count; countDown-- > 0;)
      stack.add(new TableInfo(nextId++, U));
    
    GenerationInfo young2 = GenerationInfo.candidateMerge(stack, MERGE_POLICY, 0);
    GenerationInfo yReduced2 = young2.reduceBy(yReduced1);
    
    assertEquals(young0, yReduced2);
  }
  
  
  
  private List<TableInfo> generateTableStack(long[] ids, long[] sizes) {
    ArrayList<TableInfo> stack = new ArrayList<>(ids.length);
    for (int i = 0; i < ids.length; ++i)
      stack.add(new TableInfo(ids[i], sizes[i]));
    return Collections.unmodifiableList(stack);
  }

}
