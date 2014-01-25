/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import com.gnahraf.io.Files;

/**
 * 
 * @author Babak
 */
public final class CommitRecord {
  
  /**
   * Sanity check file length limit.
   */
  private final static long MAX_COMMIT_FILE_LENGTH = 512 * 1024;
  private final static int MAX_TABLE_ID_COUNT = (int) MAX_COMMIT_FILE_LENGTH / 20;
  public final static long INIT_COMMIT_ID = 0;
  
  public final static CommitRecord INIT;
  static {
    List<Long> noIds = Collections.emptyList();
    INIT = new CommitRecord(null, noIds, INIT_COMMIT_ID);
  }
  
  private final List<Long> tableIds;
  private final File file;
  private final long commitId;
  
  

  private CommitRecord(File file, List<Long> tableIds, long commitId) {
    this.file = file;
    this.tableIds = tableIds;
    this.commitId = commitId;
  }
  
  
  /**
   * Returns the table IDs for this commit record. Tables corresponding to
   * IDs at higher indices override the lower index ones. The only guarantee
   * made by this class about the contents of this returned list is that it
   * contains no duplicates.
   * 
   * @return an unmodifiable list of table IDs.
   */
  public List<Long> getTableIds() {
    return tableIds;
  }
  
  /**
   * Returns the file this commit record was loaded from. Never <tt>null</tt>,
   * unless {@linkplain #getId() commit id} is {@linkplain #INIT_COMMIT_ID}.
   */
  public File getFile() {
    return file;
  }


  /**
   * Returns the commit ID. If the return value is {@linkplain #INIT_COMMIT_ID},
   * then this represents an empty, initial commit.
   */
  public final long getId() {
    return commitId;
  }
  
  @Override
  public String toString() {
    return "[cid=" + commitId + ", tids=" + tableIds + "]";
  }
  
  
  public static CommitRecord load(File file, long commitId) throws IOException {
    Files.assertFile(file);
    if (file.length() > MAX_COMMIT_FILE_LENGTH)
      throw new KaroonException(
          "commit file size exceeds limit: " + file.length() + "bytes");
    ArrayList<Long> tableIds = new ArrayList<>();
    String contents = Files.loadAsString(file, MAX_COMMIT_FILE_LENGTH);
    StringTokenizer idTokenizer = new StringTokenizer(contents);
    try {
      while (idTokenizer.hasMoreTokens()) {
        long id = Long.parseLong(idTokenizer.nextToken());
        tableIds.add(id);
      }
    } catch (NumberFormatException nfx) {
      throw new KaroonException(
          "failed to parse table IDs in " + file.getAbsolutePath() +
          ":\n\n" + contents + "\n");
    }
    
    // sanity check no dups..
    if (new HashSet<>(tableIds).size() != tableIds.size())
      throw new KaroonException(
          "commit file " + file.getAbsolutePath() +
          " contains duplicate table IDs:\n\n" + tableIds + "\n");
    
    // make the id list immutable
    List<Long> out;
    if (tableIds.isEmpty())
      out = Collections.emptyList();
    else {
      if (tableIds.size() > 128)
        tableIds.trimToSize();
      out = Collections.unmodifiableList(tableIds);
    }
    
    return new CommitRecord(file, out, commitId);
  }
  
  
  
  public static void write(File file, List<Long> tableIds) throws IOException {
    // sanity check size
    if (tableIds.size() > MAX_TABLE_ID_COUNT)
      throw new IllegalArgumentException("too many table IDs: " + tableIds.size());
    // sanity check no dups..
    if (new HashSet<>(tableIds).size() != tableIds.size())
      throw new IllegalArgumentException(
          "attempt to commit duplicate table IDs to " + file.getAbsolutePath() +
          ":\n\n" + tableIds + "\n");
    
    
    try (FileWriter writer = new FileWriter(file)) {
      for (Long tableId : tableIds)
        writer.append(tableId.toString()).append('\n');
    }
  }
  
  public static CommitRecord create(File file, List<Long> tableIds, long commitId) throws IOException {
    write(file, tableIds);
    return load(file, commitId);
  }

}
