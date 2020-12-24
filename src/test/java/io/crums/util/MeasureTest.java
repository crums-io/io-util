/*
 * Copyright 2006-2020 Babak Farhang
 */
package io.crums.util;


import static org.junit.Assert.*;

import org.junit.Test;

/**
 * 
 * <p><small><i>
 * From the Skwish project at sourceforge.
 * </i></small></p>
 */
public class MeasureTest {

  @Test
  public void testIsFilepath() {
    String path;

    path = "/";
    assertTrue(Measure.isFilepath(path));

    path = "abc";
    assertTrue(Measure.isFilepath(path));

    path = "/abc";
    assertTrue(Measure.isFilepath(path));

    path = "/abc/";
    assertTrue(Measure.isFilepath(path));

    path = "/abc/123";
    assertTrue(Measure.isFilepath(path));

    path = "/abc/123/";
    assertTrue(Measure.isFilepath(path));

    path = "/abc/ 123/";
    assertFalse(Measure.isFilepath(path));

    path = "/abc/123/.";
    assertTrue(Measure.isFilepath(path));

    path = "/abc/123/./abc";
    assertTrue(Measure.isFilepath(path));

    path = "/abc/123/..";
    assertTrue(Measure.isFilepath(path));

    path = "/abc/123//abc";
    assertFalse(Measure.isFilepath(path));

    path = "/abc/123/.ok.";
    assertTrue(Measure.isFilepath(path));

  }


  @Test
  public void testStartsWith() {
    assertFalse(Measure.startsWith("string", "prefix"));
    assertFalse(Measure.startsWith(null, "prefix"));
    assertFalse(Measure.startsWith("string", null));
    assertTrue(Measure.startsWith("string", ""));
    assertTrue(Measure.startsWith("", ""));
    assertTrue(Measure.startsWith("string", "s"));
    assertTrue(Measure.startsWith("string", "st"));
    assertTrue(Measure.startsWith("string", "string"));
    assertFalse(Measure.startsWith("string", "string2"));
  }

  @Test
  public void testIsSubpathOf() {
    assertTrue(Measure.isSubpathOf("", ""));
    assertTrue(Measure.isSubpathOf("/path", ""));
    assertFalse(Measure.isSubpathOf("", "/path"));
    assertTrue(Measure.isSubpathOf("/path", "/path"));
    assertFalse(Measure.isSubpathOf("/path1", "/path"));
    assertFalse(Measure.isSubpathOf("/path", "/path1"));

    assertTrue(Measure.isSubpathOf("/path1/path2", "/path1"));
  }

  @Test
  public void testIndexOf() {
    String string = " +.!kz .!+.!bf";
    String pattern = "+.!";
    subtestIndexOf(string, pattern);
    string = string.substring(0, string.length() - 2);
    subtestIndexOf(string, pattern);
  }

  private void subtestIndexOf(String string, String pattern) {
    assertEquals(1, Measure.indexOf(string, pattern));
    assertEquals(1, Measure.indexOf(string, pattern, -5));
    assertEquals(1, Measure.indexOf(string, pattern, Integer.MIN_VALUE));
    assertEquals(1, Measure.indexOf(string, pattern, Integer.MIN_VALUE + 1));
    assertEquals(1, Measure.indexOf(string, pattern, Integer.MIN_VALUE + 2));
    assertEquals(9, Measure.indexOf(string, pattern, 2));
    assertEquals(-1, Measure.indexOf(string, pattern, 10));
    assertEquals(-1, Measure.indexOf(string, pattern, string.length()));
    assertEquals(-1, Measure.indexOf(string, pattern, Integer.MAX_VALUE));
    assertEquals(-1, Measure.indexOf(string, pattern, Integer.MAX_VALUE -1));
    assertEquals(-1, Measure.indexOf(string, pattern, Integer.MAX_VALUE -2));
  }

  public void testLastIndexOf() {
    String string = " +.!kz .!+.!bf";
    String pattern = "+.!";
    subtestLastIndexOf(string, pattern);
    string = string.substring(0, string.length() - 2);
    subtestLastIndexOf(string, pattern);
  }

  private void subtestLastIndexOf(String string, String pattern) {
    assertEquals(9, Measure.lastIndexOf(string, pattern));
    assertEquals(9, Measure.lastIndexOf(string, pattern, 9));
    assertEquals(9, Measure.lastIndexOf(string, pattern, 10));
    assertEquals(9, Measure.lastIndexOf(string, pattern, string.length()));
    assertEquals(9, Measure.lastIndexOf(string, pattern, Integer.MAX_VALUE));
    assertEquals(9, Measure.lastIndexOf(string, pattern, Integer.MAX_VALUE -1));
    assertEquals(9, Measure.lastIndexOf(string, pattern, Integer.MAX_VALUE -2));
    assertEquals(1, Measure.lastIndexOf(string, pattern, 8));
    assertEquals(1, Measure.lastIndexOf(string, pattern, 2));
    assertEquals(1, Measure.lastIndexOf(string, pattern, 1));
    assertEquals(-1, Measure.lastIndexOf(string, pattern, 0));
    assertEquals(-1, Measure.lastIndexOf(string, pattern, -1));
    assertEquals(-1, Measure.lastIndexOf(string, pattern, Integer.MIN_VALUE));
    assertEquals(-1, Measure.lastIndexOf(string, pattern, Integer.MIN_VALUE + 1));
    assertEquals(-1, Measure.lastIndexOf(string, pattern, Integer.MIN_VALUE + 2));
  }

}
