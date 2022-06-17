/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;


import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class ListsTest {

  @Test
  public void testDowncast() {
    List<Integer> intList = List.of(1,2,3);
    List<Number> numList = Lists.downcast(intList);
    assertEquals(intList, numList);
  }

  @Test
  public void testDowncastToInterface() {
    List<String> sList = List.of("a","b","c");
    List<CharSequence> cList = Lists.downcast(sList);
    assertEquals(sList, cList);
  }


  @Test
  public void testUpcast() {
    List<Number> numList = List.of(1,2,3);
    List<Integer> intList = Lists.upcast(numList);
    assertEquals(intList, numList);
  }

}
