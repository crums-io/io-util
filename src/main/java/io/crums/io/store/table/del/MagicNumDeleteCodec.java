/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.del;

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
  

  @Override
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    
    if (o instanceof MagicNumDeleteCodec) {
      if (!getClass().equals(o.getClass()))
        return false;
      MagicNumDeleteCodec other = (MagicNumDeleteCodec) o;
      return offset == other.offset && magic == other.magic;
    }
    
    return false;
  }
  
  
  @Override
  public final int hashCode() {
    return getClass().hashCode() ^ offset ^ Long.hashCode(magic);
  }
  
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("magic[");
    builder.append(offset);
    builder.append(",0x");
    String hex = Long.toHexString(magic);
    if (hex.length() % 2 != 0)
      builder.append('0');
    builder.append(hex);
    builder.append("]");
    return builder.toString();
  }


  public static DeleteCodec newByteInstance(int offset, int magic) {
    if (magic < Byte.MIN_VALUE || magic > Byte.MAX_VALUE)
      throw new IllegalArgumentException("magic: "+ magic);
    return newByteInstance(offset, (byte) magic);
  }
  
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
  
  public static DeleteCodec newShortInstance(int offset, int magic) {
    if (magic < Short.MIN_VALUE || magic > Short.MAX_VALUE)
      throw new IllegalArgumentException("magic: " + magic);
    return newShortInstance(offset, (short) magic);
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
