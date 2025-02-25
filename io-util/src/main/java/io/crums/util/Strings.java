/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.TreeSet;

/**
 * String and CharSequence static utilities.
 */
public class Strings {
  
  // nobody calls
  private Strings() {  }
  
  
  public final static Charset UTF_8 = Charset.forName("UTF-8");
  
  
  /**
   * Returns the given character sequence containing no duplicate characters
   * as a string composed of its characters in lexicographic order. Introduced
   * for cannonicalizing command line options input when the order of the characters
   * doesn't matter.
   * 
   * @param charSet a sequence of characters with unique occurance
   * 
   * @return a string with the input characters in lexicographic order
   */
  public static String orderCharSet(CharSequence charSet) {
    TreeSet<Character> lexOrdererdChars = new TreeSet<>();
    for (int index = 0; index < charSet.length(); ++index) {
      char c = charSet.charAt(index);
      if (!lexOrdererdChars.add(c))
        throw new IllegalArgumentException(
            "character '" + c + "' occurs multiple times in " + charSet);
    }
    StringBuilder s = new StringBuilder(lexOrdererdChars.size());
    for (char c : lexOrdererdChars)
      s.append(c);
    return s.toString();
  }
  
  
  
  public static String nTh(long count) {
    final String suffix;
    int twoDigits = (int) (count % 100);
    if (4 <= twoDigits && twoDigits <= 20)
      suffix = "th";
    else {
      switch ((int) (count % 10)) {
      case 1:
        suffix = "st";  break;
      case 2:
        suffix = "nd";  break;
      case 3:
        suffix = "rd";  break;
      default:
        suffix = "th";
      }
    }
    return count + suffix;
  }
  

  /**
   * Returns a string of the form "n-of-something". For example:
   * <pre>
   *    System.out.println(nOf(0, "cow"));
   *    System.out.println(nOf(1, "cow"));
   *    System.out.println(nOf(2, "cow"));
   * </pre>
   * prints
   * <pre>
   *    0 cows
   *    1 cow
   *    2 cows
   * </pre>
   * 
   * @param count &ge; 0
   * @param single  the singular form of the word
   * 
   * @see #pluralize(String, long)
   */
  public static String nOf(long count, String single) {
    return count + " " + pluralize(single, count);
  }
  
  
  /**
   * Returns the given singular English word in plural form (using heuristics
   * based on ending) <em>if and only if</em> the given {@code count} is not
   * {@code 1}. (In English, zero of something is plural.)
   * 
   * <h4>Ending Patterns</h4>
   * <p>The default pluralizaton behavior is to simply append 's' to the end of the
   * input argument {@code single}. However the following endings are specially treated
   * (as per English rules-of-thumb):
   * </p>
   * <ol>
   * <li>{@code ch}</li>
   * <li>{@code sh}</li>
   * <li>{@code s}</li>
   * <li>{@code y}</li>
   * </ol>
   * 
   * @param single  the word in singular form
   * @param count   &ge; 0. If {@code 1}, then {@code single} is returned
   * 
   * @return possibly pluralized version of {@code single}
   */
  public static String pluralize(String single, long count) {
    if (count == 1)
      return single;
    if (single.endsWith("y"))
      return single.substring(0, single.length() - 1) + "ies";
    else if (single.endsWith("sh") || single.endsWith("ch") || single.endsWith("s"))
      return single + "es";
    else
      return single + "s";
  }
  
  
  /**
   * 
   * @param thirdPpp 3rd person present tense of a verb (e.g. "divide")
   * @param count &ge; 0
   * 
   * @return
   */
  public static String singularVerb(String thirdPpp, long count) {
    return count == 1 ? thirdPpp + "s" : thirdPpp;
  }
  
  
  public static boolean isWhitespace(char c) {
    switch (c) {
    case '\t':
    case '\n':
    case '\f':
    case '\r':
    case ' ':
      return true;
    default:
      return false;
    }
  }
  
  
  
  
  public static byte[] utf8Bytes(String string) {
    return string.getBytes(UTF_8);
  }
  
  
  public static ByteBuffer utf8Buffer(String string) {
    return ByteBuffer.wrap(utf8Bytes(string)).asReadOnlyBuffer();
  }
  
  
  public static String utf8String(byte[] bytes) {
    return new String(bytes, UTF_8);
  }
  
  /**
   * Returns a copy of the given buffer as a UTF-8 encoded {@code String}.
   * The positional state of the argument is not touched.
   * 
   * @param bytes copied but not touched
   */
  public static String utf8String(ByteBuffer bytes) {
    if (bytes.hasArray()) {
      return new String(
          bytes.array(),
          bytes.arrayOffset() + bytes.position(),
          bytes.remaining(),
          UTF_8);
    }
    byte[] b = new byte[bytes.remaining()];
    bytes.get(bytes.position(), b);
    return new String(b, UTF_8);
  }
  
  
  
  /**
   * Un-escapes the given escaped sequence of characters. Escaping
   * here just means c-style chars such as '\t', '\n', and so on.
   * See {@linkplain CharEscape} for the list of escaped characters.
   * 
   * @param escaped a possibly escaped char sequence (null ok)
   */
  public static String unescape(String escaped) {
    
    return CharEscape.unescape(escaped);
  }
  
  
  
  public enum CharEscape {
    /**
     * Tab '\t'.
     */
    TB('\t', "t"),
    /**
     * New line '\n'.
     */
    NL('\n', "n"),
    /**
     * Form feed '\f'.
     */
    FF('\f', "f"),
    /**
     * Carriage return '\r'.
     */
    CR('\r', "r"),
    /**
     * Back-slash '\\'.
     */
    BS('\\', "\\");
    
    
    
    public final static char BACK_SLASH = '\\';

    private final static CharEscape[] vals = values();
    
    
    
    /**
     * Escape character.
     */
    public final char eChar;
    /**
     * Escaped representation of eChar is of length 2.
     */
    public final String escaped;
    
    private CharEscape(char c, String esc) {
      this.eChar = c;
      this.escaped = '\\' + esc;
    }
    
    
    /**
     * Returns the next escape position beyond {@code fromIndex}, or null
     * if it doesn't occur.
     * 
     * @param escaped the string in escaped representation (null ok)
     */
    public static EscapePos nextEscapePos(String escaped, int fromIndex) {
      if (escaped == null || escaped.length() - 2 < fromIndex)
        return null;
      
      {
        int bsIndex = escaped.indexOf(BACK_SLASH, fromIndex);
        if (bsIndex == -1)
          return null;
        else
          fromIndex = bsIndex;
      }
      
      int min = escaped.length();
      CharEscape c = null;
      for (int ri = vals.length; ri-- > 0; ) {
        var e = vals[ri];
        int index = escaped.indexOf(e.escaped, fromIndex);
        if (index >= 0 && index < min) {
          min = index;
          c = e;
        }
      }
      
      return c == null ? null : new EscapePos(min, c);
    }
    

    /**
     * Un-escapes the given escaped sequence of characters. Escaping
     * here just means c-style chars such as '\t', '\n', and so on.
     * See {@linkplain CharEscape} for the list of escaped characters.
     * 
     * @param escaped a possibly escaped char sequence (null ok)
     */
    public static String unescape(String escaped) {
      EscapePos pos = nextEscapePos(escaped, 0);
      if (pos == null)
        return escaped;
      
      StringBuilder string = new StringBuilder(escaped.length());
      string.append(escaped.substring(0, pos.index)).append(pos.echar());
      
      while (true) {
        var nextPos = nextEscapePos(escaped, pos.index + 2);
        if (nextPos == null)
          break;
        string.append(escaped.substring(pos.index + 2, nextPos.index)).append(nextPos.echar());
        pos = nextPos;
      }
      
      if (pos.index + 2 < escaped.length())
        string.append(escaped.substring(pos.index + 2));
      
      return string.toString();
    }
  }
  
  
  public static class EscapePos {
    
    public final int index;
    public final CharEscape e;
    
    public EscapePos(int index, CharEscape e) {
      this.index = index;
      this.e = e;
      if (index < 0) throw new IllegalArgumentException("index " + index);
      Objects.requireNonNull(e, "null CharEscape");
    }
    
    public char echar() {
      return e.eChar;
    }
  }
  
  
  /**
   * Checks whether the given {@code name} qualifies as a syntactically valid
   * Java class or package name. The name is expected to be qualified with dots (<b>.</b>)
   * in the usual way, but it's not a requirement.
   * 
   * <h4>Examples</h4>
   * <p>A few example input strings with their result on invoking this method are listed below:</p>
   * <table>
   * <caption>Example Inputs/Outputs</caption>
   * <tr><th>In</th><th></th><th>Out</th></tr>
   * <tr><td>{@code abc.def.Xyz}</td><td></td><td>{@code true}</td></tr>
   * <tr><td>{@code abc.def8.X_yz}</td><td></td><td>{@code true}</td></tr>
   * <tr><td>{@code a.b}</td><td></td><td>{@code true}</td></tr>
   * <tr><td>{@code a}</td><td></td><td>{@code true}</td></tr>
   * <tr><td>{@code a.9}</td><td></td><td>{@code false}</td></tr>
   * <tr><td>{@code _bc}</td><td></td><td>{@code false}</td></tr>
   * <tr><td>{@code 9bc}</td><td></td><td>{@code false}</td></tr>
   * <tr><td>{@code a9c}</td><td></td><td>{@code true}</td></tr>
   * <tr><td>{@code abc.9ef.Xyz}</td><td></td><td>{@code false}</td></tr>
   * <tr><td>{@code .def.Xyz}</td><td></td><td>{@code false}</td></tr>
   * <tr><td>{@code abc..Xyz}</td><td></td><td>{@code false}</td></tr>
   * </table>
   * 
   * @param name    the Java entity name
   * @return {@code true} if it looks okay to be a Java type name
   */
  public static boolean isPermissableJavaName(String name) {
    if (name == null || name.isEmpty())
      return false;
    var tokenizer = new StringTokenizer(name, ".", true);
    int count = 0;
    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken();
      if ((count & 1) == 0) {
        if (!validJavaNameToken(token))
          return false;
      } else if (!token.equals("."))
        return false;
      ++count;
    }
    return (count & 1) == 1;
  }
  
  
  private static boolean validJavaNameToken(String token) {
    if (token.isEmpty())
      return false;
    if (!isAlphabet(token.charAt(0)))
      return false;
    for (int index = token.length(); index-- > 1; ) {
      char c = token.charAt(index);
      boolean ok =
          isAlphabet(c) ||
          isDigit(c) || c == '_';
      if (!ok)
        return false;
    }
    return true;
  }
  
  
  
  /**
   * @return {@code (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')}
   */
  public static boolean isAlphabet(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
  }
  
  
  
  public static boolean isAlphabetOnly(String string) {
    int i = string.length();
    while (i-- > 0 && isAlphabet(string.charAt(i)));
    return i == -1;
  }
  
  

  /**
   * @return {@code c >= '0' && c <= '9'}
   */
  public static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

}











