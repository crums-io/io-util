/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.del;

import java.nio.ByteBuffer;

/**
 * 
 * @author Babak
 */
public abstract class MagicNumDeleteCodec extends DeleteCodec {
  
  protected final int offset;
  protected final long magic;

  public MagicNumDeleteCodec(int offset, long magic) {
    this.offset = offset;
    this.magic = magic;
    if (offset < 0)
      throw new IllegalArgumentException("offset: " + offset);
  }


  @Override
  public boolean isDeleted(ByteBuffer row) {
    return magic == magicCellValue(row);
  }
  
  
  protected abstract long magicCellValue(ByteBuffer row);


  
  public static DeleteCodec newByteInstance(int offset, byte magic) {
    return new MagicNumDeleteCodec(offset, magic) {
      
      @Override
      public void markDeleted(ByteBuffer row) {
        row.put(offset, (byte) magic);
      }
      
      @Override
      protected long magicCellValue(ByteBuffer row) {
        return row.get(offset);
      }
    };
  }
  
  public static DeleteCodec newShortInstance(int offset, short magic) {
    return new MagicNumDeleteCodec(offset, magic) {
      
      @Override
      public void markDeleted(ByteBuffer row) {
        row.putShort(offset, (short) magic);
      }
      
      @Override
      protected long magicCellValue(ByteBuffer row) {
        return row.getShort(offset);
      }
    };
  }
  
  public static DeleteCodec newIntInstance(int offset, int magic) {
    return new MagicNumDeleteCodec(offset, magic) {
      
      @Override
      public void markDeleted(ByteBuffer row) {
        row.putInt(offset, (int) magic);
      }
      
      @Override
      protected long magicCellValue(ByteBuffer row) {
        return row.getInt(offset);
      }
    };
  }
  
  public static DeleteCodec newLongInstance(int offset, long magic) {
    return new MagicNumDeleteCodec(offset, magic) {
      
      @Override
      public void markDeleted(ByteBuffer row) {
        row.putLong(offset, magic);
      }
      
      @Override
      protected long magicCellValue(ByteBuffer row) {
        return row.getLong(offset);
      }
    };
  }
  
  
  

}
