/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * A generation of source tables suitable for merging.
 * Used for deciding which tables to merge.
 * 
 * @author Babak
 */
public final class GenerationInfo {
  
  private final static Logger LOG = Logger.getLogger(GenerationInfo.class);
  
  public final int generation;
  public final List<TableInfo> srcInfos;
  public final List<TableInfo> backSetInfos;
  
  private GenerationInfo(int generation, List<TableInfo> tableStack, int srcOff, int srcCount) {
    this.generation = generation;
    this.srcInfos = tableStack.subList(srcOff, srcOff + srcCount);
    if (srcOff == 0)
      backSetInfos = Collections.emptyList();
    else
      backSetInfos = tableStack.subList(0, srcOff);
  }
  
  /**
   * Returns the cumulative byte size of the sources for the merge.
   */
  public long cumulativeSourceSize() {
    long size = 0;
    for (int i = srcInfos.size(); i-- > 0; )
      size += srcInfos.get(i).size;
    return size;
  }
  
  
  /**
   * Returns a benefit to cost ratio, calculated as the number of table files reduced
   * as a result of the merge operation, divided by the {@linkplain #cumulativeSourceSize()
   * cumulative byte size of the sources for the merge}.
   * 
   * @return  <tt>((double) srcInfos.size() - 1) / cumulativeSourceSize()</tt>
   */
  public double effectToBytesRatio() {
    return ((double) srcInfos.size() - 1) / cumulativeSourceSize();
  }
  

  /**
   * Returns a possibly empty list of candidate merges in ascending generation order
   * from the given <tt>tableStack</tt> starting from the first generation. (The zeroth
   * generation is the fresh meat generation which is typically specially handled.)
   * The returned list, if not empty, is mutable.
   * <p/>
   * This is just shorthand for <tt>mergeCandidates(tableStack, mergePolicy, 1)</tt>.
   * 
   * @param tableStack
   *        list of tables in increasing order of precedence. The given list must
   *        not be modified; o.w. results are undefined.
   * @param mergePolicy
   *        used to determine generational size boundaries
   */
  public static List<GenerationInfo> generationalMergeCandidates(
      List<TableInfo> tableStack, MergePolicy mergePolicy) {
    return candidateMerges(tableStack, mergePolicy, 1);
  }
  
  /**
   * Returns a possibly empty list of candidate merges in ascending generation order
   * from the given <tt>tableStack</tt>. The returned list, if not empty, is mutable.
   * 
   * @param minGeneration
   *        the minimum {@linkplain #generation} of the returned merge candidates
   * @param tableStack
   *        list of tables in increasing order of precedence. The given list must
   *        not be modified; o.w. results are undefined.
   * @param mergePolicy
   *        used to determine generational size boundaries
   *        
   * @return a (possibly empty) list of candidate merges in ascending generation order
   */
  public static List<GenerationInfo> candidateMerges(
      List<TableInfo> tableStack, MergePolicy mergePolicy, int minGeneration) {
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("mergeCandidates: tableStack=" + tableStack);
      LOG.debug("mergeCandidates: mergePolicy=" + mergePolicy);
      LOG.debug("mergeCandidates: minGeneration=" + minGeneration);
    }
    int genStartIndex = tableStack.size();
    
    if (genStartIndex < 2)
      return Collections.emptyList();
    
    int generation = 0;
    while (generation < minGeneration) {
      long maxGenerationSize = mergePolicy.getGenerationMaxSize(generation++);
      genStartIndex = genStartIndex(tableStack, genStartIndex, maxGenerationSize);
    }

    if (genStartIndex < 2)
      return Collections.emptyList();
    
    ArrayList<GenerationInfo> mergeCandidates = new ArrayList<>();
    
    for (; genStartIndex >= 2; ++generation) {
      int genEndIndex = genStartIndex;
      long maxGenerationSize = mergePolicy.getGenerationMaxSize(generation);
//      LOG.debug("maxGenerationSize=" + maxGenerationSize);
      genStartIndex = genStartIndex(tableStack, genStartIndex, maxGenerationSize);
//      LOG.debug("genStartIndex=" + genStartIndex);
      final int count = genEndIndex - genStartIndex;
      
      if (count >= 2) {
        GenerationInfo candidate =
            new GenerationInfo(generation, tableStack, genStartIndex, count);
        mergeCandidates.add(candidate);
      }
    }
    
    return mergeCandidates;
  }

  
  private static int genStartIndex(
      List<TableInfo> tableStack, int lastGenStartIndex, long maxGenerationTableSize) {
    int index = lastGenStartIndex;
    while (index > 0) {
      if (tableStack.get(index - 1).size > maxGenerationTableSize)
        break;
      else
        --index;
    }
    return index;
  }
  
  
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    else if (o instanceof GenerationInfo) {
      GenerationInfo other = (GenerationInfo) o;
      return
          generation == other.generation &&
          srcInfos.equals(other.srcInfos) &&
          backSetInfos.equals(other.backSetInfos);
    } else
      return false;
  }
  
  
  @Override
  public int hashCode() {
    return generation;
  }
  
  
  public String toString() {
    return "[gen=" + generation + " , srcs=" + srcInfos + ", bset=" + backSetInfos + "]";
  }
  
}

