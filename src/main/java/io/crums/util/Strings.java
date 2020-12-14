/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


import java.util.TreeSet;

/**
 * String and CharSequence static utilities.
 */
public class Strings {
  
  // nobody calls
  private Strings() {  }
  
  
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
  
  
  

}
