/*
 * Copyright 2008-2020 Babak Farhang
 */
package io.crums.util;


import java.util.Collection;
import java.util.Comparator;


/**
 * Utilty for comparing and measuring various common object types and
 * primitives (such as <tt>char</tt>). Most of the methods in this class
 * have well-defined behavior when invoked with one or more <tt>null</tt>
 * parameters.
 * 
 * <p><small><i>
 * From the Skwish project at sourceforge.
 * </i></small></p>
 */
public class Measure {

  /**
   * Compares the two given objects for equality. A <tt>null</tt> reference is
   * considered equal only to itself.
   */
  public static boolean equal(Object a, Object b) {
    if (a == null)
      return b == null;
    return a.equals(b);
  }

  /**
   * Determines whether the given <tt>array</tt> is empty.
   * A <tt>null</tt> reference is considered empty.
   */
  public static boolean isEmpty(Object[] array) {
    return array == null | array.length == 0;
  }


  /**
   * Determines whether the given <tt>string</tt> is empty.
   * A <tt>null</tt> reference is considered empty.
   */
  public static boolean isEmpty(CharSequence string) {
    return string == null || string.length() == 0;
  }


  /**
   * Determines whether the given <tt>collection</tt> is empty.
   * A <tt>null</tt> reference is considered empty.
   */
  public static boolean isEmpty(Collection<?> collection) {
    return collection == null || collection.isEmpty();
  }

  /**
   * Returns the length of the given <tt>string</tt>.
   * If a <tt>null</tt> reference, then -1 is returned.
   */
  public static int getLength(CharSequence string) {
    return string == null ? -1 : string.length();
  }

  /**
   * Determines whether the given <tt>string</tt> starts with the given
   * <tt>start</tt> sequence. If either of the input parameters is
   * <tt>null</tt>, then <tt>false</tt> is returned.
   */
  public static boolean startsWith(CharSequence string, CharSequence start)
  {
    if (string == null || start == null || start.length() > string.length())
      return false;
    int i = start.length();
    while (i-- > 0 && start.charAt(i) == string.charAt(i));
    return i == -1;
  }


  /**
   * Returns the index of the first occurrence of <tt>c</tt> in
   * <tt>string</tt>, if found; returns -1, otherwise.
   * 
   * @param string
   *        the char sequence to be searched.  Must not be <tt>null</tt>.
   */
  public static int indexOf(CharSequence string, char c) {
    return indexOf(string, c, 0);
  }


  /**
   * Returns the index of the first occurrence of <tt>c</tt> in
   * <tt>string</tt> that is &ge; <tt>fromIndex</tt>, if found; returns
   * -1, otherwise.
   * 
   * @param string
   *        the char sequence to be searched.  Must not be <tt>null</tt>.
   */
  public static int indexOf(CharSequence string, char c, int fromIndex) {
    int index = Math.max(0, fromIndex);
    final int len = string.length();
    while (index < len) {
      if (string.charAt(index) == c)
        return index;
      else
        ++index;
    }
    return -1;
  }


  /**
   * Returns the index of the first <em>space</em> character in the given
   * <tt>string</tt>, starting from <tt>fromIndex</tt>, if found; returns
   * -1, otherwise. The space characters are recognized by this method are
   * ' ', '\n', '\r', '\t', and '\f'.
   * 
   * @param string
   *        the char sequence to be searched.  Must not be <tt>null</tt>.
   */
  public static int indexOfSpace(CharSequence string, int fromIndex) {
    int index = Math.max(0, fromIndex);
    final int len = string.length();
    while (index < len) {
      switch (string.charAt(index)) {
      case ' ':
      case '\n':
      case '\r':
      case '\t':
      case '\f':
        return index;
      default:
        ++index;
      }
    }
    return -1;
  }


  /**
   * Returns the index of the first <em>non-space</em> character in the given
   * <tt>string</tt>, starting from <tt>fromIndex</tt>, if found; returns
   * -1, otherwise. The space characters are recognized by this method are
   * ' ', '\n', '\r', '\t', and '\f'.
   * 
   * @param string
   *        the char sequence to be searched.  Must not be <tt>null</tt>.
   */
  public static int indexOfNonspace(CharSequence string, int fromIndex) {
    int index = Math.max(0, fromIndex);
    final int len = string.length();
    while (index < len) {
      switch (string.charAt(index)) {
      case ' ':
      case '\n':
      case '\r':
      case '\t':
      case '\f':
        ++index;
        break;  // ..out of switch
      default:
        return index;
      }
    }
    return -1;
  }


  /**
   * Returns the index of the first occurrence of the specified <tt>pattern
   * </tt> in the given <tt>string</tt>, if found; returns -1, otherwise.
   * <p/>
   * Neither of the arguments may be <tt>null</tt>.
   */
  public static int indexOf(CharSequence string, CharSequence pattern) {
    return indexOf(string, pattern, 0);
  }


  /**
   * Returns the index of the first occurrence of the specified <tt>pattern
   * </tt> in the given <tt>string</tt> that is &ge; to <tt>fromIndex</tt>,
   * if found; returns -1, otherwise.
   * <p/>
   * Neither of the arguments may be <tt>null</tt>.
   */
  public static int indexOf(
      CharSequence string, CharSequence pattern, int fromIndex)
  {
    final int len = pattern.length();
    int index = Math.max(0, fromIndex) + len;
    if (index < 0)
      return -1;
    final int max = string.length() + 1;
    while (index < max) {
      int i = len;
      int j = index;
      while (i-- > 0 && string.charAt(--j) == pattern.charAt(i));
      if (i == -1)
        return j;
      ++index;
    }
    return -1;
  }


  /**
   * Returns the last index of the character <tt>c</tt> in the given
   * <tt>string</tt>, if found; returns -1, otherwise.
   * 
   * @param string
   *        the char sequence to be searched.  Must not be <tt>null</tt>.
   */
  public static int lastIndexOf(CharSequence string, char c) {
    return lastIndexOfImpl(string, c, string.length());
  }


  /**
   * Returns the last index of the character <tt>c</tt> in the given
   * <tt>string</tt> that is &le; <tt>fromIndex</tt>, if found; returns -1,
   * otherwise.
   * 
   * @param string
   *        the char sequence to be searched.  Must not be <tt>null</tt>.
   */
  public static int lastIndexOf(CharSequence string, char c, int fromIndex) {
    int index = fromIndex < 0 ? 0 : Math.min(fromIndex + 1, string.length());
    return lastIndexOfImpl(string, c, index);
  }


  /**
   * @param index
   *        exclusive start index
   */
  private static int lastIndexOfImpl(CharSequence string, char c, int index) {
    while (index-- > 0 && string.charAt(index) != c);
    return index;
  }


  /**
   * Returns the last index of the specified <tt>pattern</tt> in the given
   * <tt>string</tt>, if found; returns -1, otherwise.
   * <p/>
   * Neither of the arguments may be <tt>null</tt>.
   */
  public static int lastIndexOf(CharSequence string, CharSequence pattern) {
    return lastIndexOf(string, pattern, string.length());
  }

  /**
   * Returns the last index of the specified <tt>pattern</tt> in the given
   * <tt>string</tt> that is &le; <tt>fromIndex</tt>, if found; returns -1,
   * otherwise.
   * <p/>
   * Neither of the arguments may be <tt>null</tt>.
   */
  public static int lastIndexOf(
      CharSequence string, CharSequence pattern, int fromIndex)
  {
    final int len = pattern.length();
    int index;
    {
      final int sLen = string.length();
      if (fromIndex > sLen -len)
        index = sLen + 1;
      else
        index = fromIndex + len + 1;
    }

    while (index-- > len) {
      int i = len;
      int j = index;
      while (i-- > 0 && string.charAt(--j) == pattern.charAt(i));
      if (i == -1)
        return j;
    }
    return -1;
  }


  /**
   * Returns the length of the given <tt>array</tt> if not <tt>null</tt>;
   * -1, otherwise.
   */
  public static int getLength(Object[] array) {
    return array == null ? -1 : array.length;
  }

  /**
   * Returns the size of the given <tt>collection</tt> if not <tt>null</tt>;
   * -1, otherwise.
   */
  public static int getSize(Collection<?> collection) {
    return collection == null ? -1 : collection.size();
  }






  /**
   * Returns the length of the prefix shared by the two given strings (never
   * negative).
   */
  public static int getCommonPrefixLength(CharSequence a, CharSequence b) {
    final int maxLen = Math.min( getLength(a), getLength(b) );
    int i = 0;
    while (i < maxLen && a.charAt(i) == b.charAt(i))
      ++i;
    return i;
  }



  /**
   * Determines whether the given <tt>subpath</tt> is in fact a subpath
   * of the specified <tt>parent</tt> path. Slash ('/')
   * is used as the default separator character.
   */
  public static boolean isSubpathOf(
      CharSequence subpath, CharSequence parent)
  {
    return isSubpathOf(subpath, parent, '/');
  }

  /**
   * Determines whether the given <tt>subpath</tt> is in fact a subpath
   * of the specified <tt>parent</tt> path.
   * 
   * @param sep
   *        the separator character (usually '/')
   */
  public static boolean isSubpathOf(
      CharSequence subpath, CharSequence parent, char sep)
  {
    if (startsWith(subpath, parent)) {
      if (subpath.length() == parent.length())
        return true;
      final char firstDiff = subpath.charAt(parent.length());
      if (firstDiff == sep)
        return true;
      if (subpath.charAt(parent.length() - 1) == sep)
        return true;
    }
    return false;
  }

  public static boolean isSubpathChar(char ch) {
    boolean valid
    = isLowerCaseAscii(ch) || isUpperCaseAscii(ch) || isDigit(ch);

    if (!valid) {
      switch (ch) {
      case '-':
      case '.':
      case '_':
        valid = true;
      }
    }

    return valid;
  }

  public static boolean isDigit(char ch) {
    switch (ch) {
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
      return true;
    default:
      return false;
    }
  }

  public static boolean isLowerCaseAscii(char ch) {
    switch (ch) {
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
      return true;
    default:
      return false;
    }
  }

  public static boolean isUpperCaseAscii(char ch) {
    switch (ch) {
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
      return true;
    default:
      return false;
    }
  }

  public static boolean isFilepath(CharSequence path) {
    return isFilepath(path, '/');
  }

  public static boolean isFilepath(CharSequence path, final char seperator)
  {
    final int len = Measure.getLength(path);

    if (len < 1)
      return false;

    boolean valid = true;
    boolean validSubpath = true;

    for (int i = 0; valid && i < len; ++i)
    {
      final char ch = path.charAt(i);

      if (ch == seperator) {
        if (validSubpath)
          validSubpath = false;
        else
          valid = false;
      } else {
        if (isSubpathChar(ch))
          validSubpath = true;
        else
          valid = false;
      }

    }

    return valid;
  }


  public static <T extends Comparable<T>> Comparator<T> naturalComparator() {
    return new Comparator<T>() {
      public int compare(T t1, T t2) {
        return t1.compareTo(t2);
      }
    };
  }


  private Measure() { }

}

