/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;

import java.io.File;

import com.gnahraf.io.store.karoon.merge.MergePolicy;
import com.gnahraf.io.store.table.del.DeleteCodec;
import com.gnahraf.io.store.table.order.RowOrder;

/**
 * 
 * @author Babak
 */
public class TStoreConfig {
  
  private final RowOrder rowOrder;
  private final int rowWidth;
  private final DeleteCodec deleteCodec;
  private final File rootDir;
  private final MergePolicy policy;
  
  
  public TStoreConfig(RowOrder rowOrder, int rowWidth, DeleteCodec deleteCodec, File rootDir, MergePolicy policy)
      throws IllegalArgumentException {
    this.rowOrder = rowOrder;
    this.rowWidth = rowWidth;
    this.deleteCodec = deleteCodec;
    this.rootDir = rootDir;
    this.policy = policy;
    
    if (rowOrder == null)
      throw new IllegalArgumentException("null rowOrder");
    if (rowWidth < 1)
      throw new IllegalArgumentException("rowWidth: " + rowWidth);
    if (rootDir == null)
      throw new IllegalArgumentException("null rootDir");
    if (policy == null)
      throw new IllegalArgumentException("null policy");
  }
  
  
  
  public final RowOrder getRowOrder() {
    return rowOrder;
  }



  public final int getRowWidth() {
    return rowWidth;
  }


  /**
   * Returns the optional deleteCodec. If this method returns <tt>null</tt>,
   * then deletes will not be supported in the <tt>TStore</tt>.
   */
  public final DeleteCodec getDeleteCodec() {
    return deleteCodec;
  }



  public final File getRootDir() {
    return rootDir;
  }
  
  
  public final MergePolicy getMergePolicy() {
    return policy;
  }
  
  
  



  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("[rowOrder=").append(rowOrder);
    builder.append(", rowWidth=").append(rowWidth);
    builder.append(", deleteCodec=").append(deleteCodec);
    builder.append(", rootDir=").append(rootDir);
    builder.append(", policy=").append(policy);
    builder.append("]");
    return builder.toString();
  }



  public static Builder newBuilder() {
    return new Builder();
  }


  /**
   * Builds
   * 
   * @author Babak
   */
  public static class Builder {
    
    private RowOrder rowOrder;
    private int rowWidth;
    private DeleteCodec deleteCodec;
    private File rootDir;
    private MergePolicy policy;
    
    
    public RowOrder getRowOrder() {
      return rowOrder;
    }
    public Builder setRowOrder(RowOrder rowOrder) {
      this.rowOrder = rowOrder;
      return this;
    }
    
    public int getRowWidth() {
      return rowWidth;
    }
    public Builder setRowWidth(int rowWidth) {
      this.rowWidth = rowWidth;
      return this;
    }
    
    public DeleteCodec getDeleteCodec() {
      return deleteCodec;
    }
    public Builder setDeleteCodec(DeleteCodec deleteCodec) {
      this.deleteCodec = deleteCodec;
      return this;
    }
    
    public File getRootDir() {
      return rootDir;
    }
    public Builder setRootDir(File rootDir) {
      this.rootDir = rootDir;
      return this;
    }
    
    public MergePolicy getMergePolicy() {
      return policy;
    }
    public Builder setMergePolicy(MergePolicy policy) {
      this.policy = policy;
      return this;
    }
    
    public TStoreConfig toConfig() throws IllegalArgumentException {
      return new TStoreConfig(rowOrder, rowWidth, deleteCodec, rootDir, policy);
    }
    
  }

}
