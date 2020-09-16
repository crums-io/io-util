/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.karoon.merge;


import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.crums.util.CollectionUtils;


/**
 * A generation of source tables suitable for merging.
 * Used for deciding which tables to merge.
 * 
 * @author Babak
 */
public final class GenerationInfo {
  
  private final static Logger LOG = Logger.getLogger(GenerationInfo.class.getName());
  
  public final int generation;
  public final List<TableInfo> srcInfos;
  public final List<TableInfo> backSetInfos;

  
  private GenerationInfo(int generation, List<TableInfo> srcInfos, List<TableInfo> backSetInfos) {
    this.generation = generation;
    this.srcInfos = srcInfos;
    this.backSetInfos = backSetInfos;
    if (srcInfos.size() < 2)
      throw new RuntimeException("Assertion failied: this=" + this);
  }
  
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
   * 
   * @param pg
   *        a prior run's generation info. Must be of the same generation as this instance.
   * @return this instance, if <tt>pg</tt>
   *         is orthogonal to this instance (its doesn't overlap with this instance's
   *         {@linkplain #srcIds()}); a reduced instance, if <tt>pg</tt>'s sources
   *         overlap this instance but leave at least 2 contiguous sources to merge; or
   *         <tt>null</tt>, o.w.
   *         
   * @throws NullPointerException
   *         if argument is <tt>null</tt>
   * @throws IllegalArgumentException
   *         if <tt>pg</tt>'s {@link #generation} doesn't match this instance's
   * @throws HistoryException
   *         if <tt>pg</tt>'s history or state (e.g. if it only partially overlaps this instance)
   */
  public GenerationInfo reduceBy(GenerationInfo pg) throws HistoryException {
    if (pg.generation != generation)
      throw new IllegalArgumentException("generation mismatch: this=" + this + "; pg=" + pg);
    
    List<Long> priorSrcIds = pg.srcIds();
    
    
    // get the srcIds .. hang on to them
    List<Long> srcIds = srcIds();
    
    // note the back of the priorSrcIds stack
    final int firstIndexOfPrior = srcIds.indexOf(priorSrcIds.get(0));
    // aside: under the current merge implementation, firstIndexOfPrior will always be
    // either 0 or -1. We'll pretend not to know that so we can safely tinker the merge
    // impl later
    
    if (firstIndexOfPrior == -1) {
      // since the back of the priorSrcIds stack was not found, then none of it should be found
      // assert this requirement..
      for (int index = 1; index < priorSrcIds.size(); ++index)
        if (srcIds.contains(priorSrcIds.get(index)))
          throw new HistoryException(
              "inconsistent historical table stack. this=" + this + "; pg=" + pg);
      return this;
    }
    
    // since the back  of the priorSrcIds stack was found in the srcIds stack
    // the rest of it must also be here (in srcIds) in the same order
    // assert this requirement..
    if (!srcIds.subList(
        firstIndexOfPrior,
        Math.min( srcIds.size(), firstIndexOfPrior + priorSrcIds.size() ))
        .equals(priorSrcIds))
      throw new HistoryException(
          "inconsistent historical table stack. this=" + this + "; pg=" + pg);
    
    final int backSize = firstIndexOfPrior;
    final int frontSize = srcIds.size() - firstIndexOfPrior - priorSrcIds.size();
    final boolean useFront = frontSize >= backSize;
    final int start, end;
    if (useFront) {
      start = srcIds.size() - frontSize;
      end = srcIds.size();
    } else {
      start = 0;
      end = backSize;
    }
    
    // if there's nothing left to merge
    if (end - start < 2)
      return null;
    
    
    final List<TableInfo> newBackSet, newSrcInfos = srcInfos.subList(start, end);
    if (useFront) {
      ArrayList<TableInfo> bset = new ArrayList<>(backSetInfos.size() + start);
      bset.addAll(backSetInfos);
      bset.addAll(srcInfos.subList(0, start));
      newBackSet = Collections.unmodifiableList(bset);
    } else
      newBackSet = backSetInfos;
    
    return new GenerationInfo(generation, newSrcInfos, newBackSet);
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
   * Returns  a candidate merge for the specified generation <tt>g</tt>, or <tt>null</tt>
   * if there's nothing to merge.
   * <p/>
   * This is just shorthand for <tt>candidateMerges(tableStack, mergePolicy, g, g + 1)</tt>.
   * 
   * @param tableStack
   *        list of tables in increasing order of precedence. The given list must
   *        not be modified; o.w. results are undefined.
   * @param mergePolicy
   *        used to determine generational size boundaries
   * @param g
   *        the generation number of the returned merge candidate
   */
  public static GenerationInfo candidateMerge(List<TableInfo> tableStack, MergePolicy mergePolicy, int g) {
    if (g < 0)
      throw new IllegalArgumentException("g: " + g);
    List<GenerationInfo> generation = candidateMerges(tableStack, mergePolicy, g, g + 1);
    return generation.isEmpty() ? null : generation.get(0);
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
    return candidateMerges(tableStack, mergePolicy, minGeneration, Integer.MAX_VALUE);
  }
  
  
  
  /**
   * Returns a possibly empty list of candidate merges in ascending generation order
   * from the given <tt>tableStack</tt>. The returned list, if not empty, is mutable.
   * 
   * @param tableStack
   *        list of tables in increasing order of precedence. The given list must
   *        not be modified; o.w. results are undefined.
   * @param mergePolicy
   *        used to determine generational size boundaries
   * @param minGeneration
   *        the minimum {@linkplain #generation} of the returned merge candidates (inclusive)
   * @param maxGeneration
   *        the maximum {@linkplain #generation} of the returned merge candidates (exclusive)
   *        
   * @return a (possibly empty) list of candidate merges in ascending generation order
   * 
   * @throws IllegalArgumentException
   *         if (minGeneration &lt; 0 || maxGeneration &lt; minGeneration)
   */
  public static List<GenerationInfo> candidateMerges(
      List<TableInfo> tableStack, MergePolicy mergePolicy, int minGeneration, int maxGeneration) {
    
    if (LOG.isLoggable(Level.FINE)) {
      LOG.fine("mergeCandidates: tableStack=" + tableStack);
      LOG.fine("mergeCandidates: mergePolicy=" + mergePolicy);
      LOG.fine("mergeCandidates: minGeneration=" + minGeneration);
      LOG.fine("mergeCandidates: maxGeneration=" + maxGeneration);
    }
    
    if (minGeneration < 0 || maxGeneration < minGeneration)
      throw new IllegalArgumentException(
          "min/max generation: " + minGeneration + "/" + maxGeneration);
    
    
    // Too bad there I have to do this (it's a public method)..
    // (this is to guarantee our instances are immutable)
    tableStack = CollectionUtils.readOnlyCopy(tableStack);
    
    int generation = 0;
    // the index at which a generation begins in the tableStack
    // set this to that of the latest generation examined (-1)
    int genStartIndex = tableStack.size();  // remember, the top of the stack is the back of the list
    
    if (genStartIndex < 2)
      return Collections.emptyList();
    
    while (generation < minGeneration) {
      long maxGenerationSize = mergePolicy.getGenerationMaxSize(generation++);
      genStartIndex = genStartIndex(tableStack, genStartIndex, maxGenerationSize);
    }

    if (genStartIndex < 2)
      return Collections.emptyList();
    
    ArrayList<GenerationInfo> mergeCandidates = new ArrayList<>();
    
    for (; generation < maxGeneration && genStartIndex >= 2; ++generation) {
      final int genEndIndex = genStartIndex;
      final long maxGenerationSize = mergePolicy.getGenerationMaxSize(generation);
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
  
  
  /**
   * Two instances are equal if their contents are equal.
   */
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
  

  @Override
  public String toString() {
    return "[gen=" + generation + " , srcs=" + srcInfos + ", bset=" + backSetInfos + "]";
  }

  public List<Long> srcIds() {
    return new AbstractList<Long>() {
      @Override
      public Long get(int index) {
        return srcInfos.get(index).tableId;
      }
      @Override
      public int size() {
        return srcInfos.size();
      }
    };
  }
  
  
  public List<Long> backSetIds() {
    if (backSetInfos.isEmpty())
      return Collections.emptyList();
    else {
      return
          new AbstractList<Long>() {
            @Override
            public Long get(int index) {
              return backSetInfos.get(index).tableId;
            }
            @Override
            public int size() {
              return backSetInfos.size();
            }
          };
    }
  }
  
}

