/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


import java.nio.charset.Charset;
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
  
  
  
  public static String nTh(int count) {
    switch (count) {
    case 1:
      return "1st";
    case 2:
      return "2nd";
    case 3:
      return "3rd";
    default:
      return count + "th";
    }
  }
  

  
  
  public static String pluralize(String single, long count) {
    if (count == 1)
      return single;
    if (single.endsWith("y"))
      return single.substring(0, single.length() - 1) + "ies";
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
  

}
