/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

/**
 * 
 */
public class IntegralStrings {
  
  private IntegralStrings() {  }
  
  

  
  private final static String[] BYTE_HEX_VALS;
  
  /**
   * Ordered list of 2 character wide, lowercase hex strings representing
   * the byte values from 0 to 255. Immutable.
   */
  public final static List<String> BYTE_HEX_VALUES;
  
  static {
    BYTE_HEX_VALS = new String[256];
    for (int i = 0; i < 16; ++i)
      BYTE_HEX_VALS[i] = "0" + Integer.toHexString(i);
    for (int i = 16; i < 256; ++i)
      BYTE_HEX_VALS[i] = Integer.toHexString(i);
    
    BYTE_HEX_VALUES = Lists.asReadOnlyList(BYTE_HEX_VALS);
  }
  
  
  
  public static boolean isHex(String number) {
    int i = number.length();
    if (i == 0 || (i & 1) != 0)
      return false;
    
    while (i-- > 0) {
      char c = number.charAt(i);
      if (c < '0')
        break;
      if (c <= '9')
        continue;
      
      if (c < 'A')
        break;
      if (c <= 'F')
        continue;
      
      if (c < 'a' || c > 'f')
        break;
    }
    return i == -1;
  }
  
  
  
  public static boolean isHexDigit(char c) {
    switch (c) {
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
    case 'A':
    case 'B':
    case 'C':
    case 'D':
    case 'E':
    case 'F':
    case 'a':
    case 'b':
    case 'c':
    case 'd':
    case 'e':
    case 'f':
      return true;
    }
    return false;
  }
  
  
  public static boolean isLowercaseHex(String number) {
    int i = number.length();
    if (i == 0)
      return false;
    
    while (i-- > 0) {
      char c = number.charAt(i);
      if (c < '0')
        break;
      if (c <= '9')
        continue;
      
//      if (c < 'A')
//        break;
//      if (c <= 'F')
//        continue;
      
      if (c < 'a' || c > 'f')
        break;
    }
    return i == -1;
  }
  
  
  public static String canonicalizeHex(String hex) {
    if (!isHex(hex))
      throw new IllegalArgumentException(hex);
    
    return hex.toLowerCase(Locale.ROOT);
  }

  
  public static byte[] hexToBytes(CharSequence hex) {
    return hexToBytes(hex, null);
  }
  
  public static byte[] hexToBytes(CharSequence hex, byte[] out) {
    int len = hex.length();
    if ((len & 1) != 0)
      throw new IllegalArgumentException("odd hex string length: " + hex);
    if (out == null)
      out = new byte[len / 2];
    else if (out.length != len / 2)
      throw new IllegalArgumentException("out.length != " + (len / 2) + " (hex.length()/2)");
    
    for (int index = 0; index < len; index += 2) {
      int hi = Character.digit(hex.charAt(index), 16);
      int lo = Character.digit(hex.charAt(index + 1), 16);
      
      if (hi == -1 || lo == -1)
        throw new IllegalArgumentException("not hex: " + hex);
      
      out[index / 2] = (byte) ((hi << 4) + lo);
    }
    
    return out;
  }
  
  
  public static String toHex(byte b) {
    return BYTE_HEX_VALS[((int) b) & 0xff];
  }
  
  
  public static String toHex(byte[] bytes) {
    StringBuilder string = new StringBuilder(2 * bytes.length);
    return appendHex(bytes, string).toString();
  }
  
  
  public static StringBuilder appendHex(byte[] bytes, StringBuilder string) {
    int len = bytes.length;
    if (len == 0)
      throw new IllegalArgumentException("empty array: " + bytes);
    
    for (int i = 0; i < len; ++i)
      string.append(toHex(bytes[i]));
    return string;
  }
  
  
  /**
   * Returns a hex string representing the remaining contents of the
   * given buffer. The positional state of the buffer is never modified.
   * 
   * @param buffer  with remaining bytes
   * 
   * @return lowercase hex string
   */
  public static String toHex(ByteBuffer buffer) {
    StringBuilder string = new StringBuilder(2 * buffer.remaining());
    return appendHex(buffer, string).toString();
  }
  

  
  
  public static StringBuilder appendHex(ByteBuffer buffer, StringBuilder string) {
    int len = buffer.remaining();
    if (len == 0)
      throw new IllegalArgumentException("empty buffer: " + buffer);
    
    for (int i = buffer.position(); i < buffer.limit(); ++i)
      string.append(toHex(buffer.get(i)));
    return string;
    
  }

}
