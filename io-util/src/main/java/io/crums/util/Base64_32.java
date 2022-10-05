/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;


import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A Base64 encoder for 32 byte strings. Encodes to 43 characters. This uses the
 * <b>URL</b> / <b>filename-friendly</b> character-set specified in
 * <a href="https://tools.ietf.org/html/rfc4648#section-5">&sect;5 of RFC 4648</a> but
 * takes liberties (discussed tho not condoned in
 * <a href="https://tools.ietf.org/html/rfc4648#section-3.2">&sect;3.2</a>) for
 * specifying boundary conditions in a way that does not require padding for our use case.
 * 
 * 
 * <h2>Format</h2>
 * <p>
 * As usual the encoding consists of mapping a sequence of 8-bit bytes to a
 * sequence of 6-bit data. We have 43 of these 6-bit sequences, with the first
 * 6-bit sequence initialized as follows:
 * </p><p>
 * Consider a protocol in which we <em>prepend</em> every 32-byte sequence with an extra
 * 0 byte: this 33-byte sequence would map to 44 base64 characters exactly. The
 * <em>first</em> character in every such base64 sequence would be redundant however,
 * since its value would be identically zero. Our protocol, then, requires that we drop
 * this leftmost zero.
 * </p><p>
 * More concretely consider a queue of bits added to on the right in chunks of 8 (byte) and consumed
 * on the left in chunks of 6 (base 64). The initialization, then, consists of filling this
 * bit-queue with 2 zero bits (followed by 8 bits from the first byte).
 * </p>
 * <h3>One-to-One</h3>
 * <p>
 * Implementation-wise, the first 2 bits <em>could</em> be ignored (i.e. set them to
 * zero even if the base64 character read does not.) However, we <b>require padding bits be zero</b>
 * in order maintain a one-to-one mapping.
 * </p>
 * <h2>Pros &amp; Cons</h2>
 * <p>
 * Compiled here to inform possible adoption.
 * </p>
 * <h3>Pros</h3>
 * 
 * <ul>
 * <li><em>Compact</em>. Nearly 50% savings compared to hex.</li>
 * <li><em>URL friendly</em>. Does not need URL encoding.</li>
 * </ul>
 * 
 * <h3>Cons</h3>
 * 
 * <ul>
 * <li><em>Broken Doubleclick Selection</em>. Doubleclicking selects the whole number
 * as one word if it's all alphanumeric; the '-' character is not alphanumeric, so
 * it breaks the selection.</li>
 * <li>Not widely known.</li>
 * </ul>
 * 
 * 
 * 
 * @see <a href="https://en.bitcoin.it/wiki/Base58Check_encoding">Base58Check encoding</a>
 */
public class Base64_32 {
  
  /**
   * The number of base64 characters a 32-byte string is mapped to.
   */
  public final static int ENC_LEN = 43; // ceiling(32 * 8 / 6)

  private final static char[] CHARS  = {
    '0', '1', '2', '3','4', '5', '6', '7', '8', '9', 
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
    'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
    'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
    'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
    'y', 'z', '-', '_'
  };
  
  
  
  /**
   * The symbols used as digits, in ascending order.
   */
  public final static String SYMBOLS = new String(CHARS);
  
  
  
  
  
  /**
   * Encodes and returns the given 32-byte sequence as a 43-character base64 string.
   */
  public static String encode(byte[] data) {
    if (Objects.requireNonNull(data).length != 32)
      throw new IllegalArgumentException("expected 32 bytes; actual is " + data.length + " bytes");
    
    StringBuilder out = new StringBuilder(43);
    
    
    short twoBytes = (short) ((0xff & data[0]) << 6);
    int winBitIndex = 10; // frontier index (from left)
    int index = 0;
    final int rightMask = 0b111111; // rightmost 6 bits
    while (true) {
      int cIndex = (twoBytes >> 10) & rightMask;
      out.append(CHARS[cIndex]);
      if (out.length() == ENC_LEN)
        break;
      twoBytes <<= 6;
      winBitIndex -= 6;
      if (winBitIndex < 6) {
        int b = (0xff & data[++index]) << (8 - winBitIndex);
        twoBytes |= b;
        winBitIndex += 8;
      }
    }
    
    return out.toString();
  }
  
  

  /**
   * Encodes and returns the next 32 bytes in the given data as a 43-character base64 string.
   * On return, the position of the given buffer is advanced by 32 bytes.
   */
  public static String encodeNext32(ByteBuffer data) {
    if (Objects.requireNonNull(data).remaining() < 32)
      throw new IllegalArgumentException("expected at least 32 bytes; actual is " + data.remaining() + " bytes");
    
    StringBuilder out = new StringBuilder(43);
    
    final int pos = data.position();
    
    short twoBytes = (short) ((0xff & data.get(pos + 0)) << 6);
    int winBitIndex = 10; // frontier index (from left)
    int index = 0;
    final int rightMask = 0b111111; // rightmost 6 bits
    while (true) {
      int cIndex = (twoBytes >> 10) & rightMask;
      out.append(CHARS[cIndex]);
      if (out.length() == ENC_LEN)
        break;
      twoBytes <<= 6;
      winBitIndex -= 6;
      if (winBitIndex < 6) {
        ++index;
        int b = ((0xff & data.get(pos + index)) << (8 - winBitIndex));
        twoBytes |= b;
        winBitIndex += 8;
      }
    }
    
    data.position(pos + 32);
    
    return out.toString();
  }
  
  
  

  /**
   * Decodes and returns the given 43-character base64 string as a 32-byte sequence.
   */
  public static byte[] decode(CharSequence base64) {
    return decode(base64, new byte[32], 0);
  }
  
  
  /**
   * Decodes and writes the given 43-character base64 string into the given
   * {@code out} array as a 32-byte sequence starting from the given {@code offset}.
   * 
   * @param base64
   * @param out an array big enough so that there are at least 32 bytes beyond {@code offset}
   * @param offset &ge; 0
   * 
   * @return the {@code out} argument
   */
  public static byte[] decode(CharSequence base64, byte[] out, int offset) {
    
    // boilerplate arg checks
    if (Objects.requireNonNull(base64, "null base64").length() != ENC_LEN)
      throw new IllegalArgumentException(
          "expected " + ENC_LEN + " chars; actual given is " + base64.length() + ": " + base64);
    
    if (Objects.requireNonNull(out, "null out").length - offset < 32)
      throw new IllegalArgumentException(
          "out.length " + out.length + " - offset " + offset + " < 32");
    
    if (offset < 0)
      throw new IllegalArgumentException("offset " + offset);
    
    
    
    short twoBytes;
    {
      char c = base64.charAt(0);
      int sixBitVal = sixBitValue(c);// (sixBitValue(base64.charAt(0)) << 12);
      // even tho we drop the 2-highest bits of the first 6 bit value,
      // we want to ensure a one-to-one mapping, so we enforce that these bits
      // must be zero:
      if (sixBitVal > 0b001111) {
        throw new IllegalArgumentException("Illegal first char '" + c + "'");
      }
        
      twoBytes = (short) (sixBitVal << 12);
    }
    int winBitIndex = 4;
    int outIndex = offset;
    for (int index = 1; index < ENC_LEN; ++index) {
      
      int sixBitVal = sixBitValue(base64.charAt(index)) << (10 - winBitIndex);
      
      twoBytes |= sixBitVal;
      winBitIndex += 6;
      if (winBitIndex >= 8) {
        if (index != 0)
          out[outIndex++] = (byte) ((0xff00 & twoBytes) >> 8);
        twoBytes <<= 8;
        winBitIndex -= 8;
      }
    }
    return out;
  }
  
  
  private static int sixBitValue(char c) {
    switch (c) {
    case '-':
      return 62;
    
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
      return c - '0';
    
    case 'A':
    case 'B':
    case 'C':
    case 'D':
    case 'E':
    case 'F':
    case 'G':
    case 'H':
    case 'I':
    case 'J':
    case 'K':
    case 'L':
    case 'M':
    case 'N':
    case 'O':
    case 'P':
    case 'Q':
    case 'R':
    case 'S':
    case 'T':
    case 'U':
    case 'V':
    case 'W':
    case 'X':
    case 'Y':
    case 'Z':
      return c - 'A' + 10;
    
    case '_':
      return 63;
    
    case 'a':
    case 'b':
    case 'c':
    case 'd':
    case 'e':
    case 'f':
    case 'g':
    case 'h':
    case 'i':
    case 'j':
    case 'k':
    case 'l':
    case 'm':
    case 'n':
    case 'o':
    case 'p':
    case 'q':
    case 'r':
    case 's':
    case 't':
    case 'u':
    case 'v':
    case 'w':
    case 'x':
    case 'y':
    case 'z':
      return c - 'a' + 36;
    default:
      throw new IllegalArgumentException("char '" + c + "' is not a Base64 digit");
    }
  }

  private Base64_32() {  }

}
