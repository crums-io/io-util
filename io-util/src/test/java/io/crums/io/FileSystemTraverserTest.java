/*
 * Copyright 2008-2020 Babak Farhang
 */
package io.crums.io;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.crums.testing.IoTestCase;

import io.crums.util.tree.TraverseListener;

/**
 * 
 */
public class FileSystemTraverserTest extends IoTestCase {

  private static abstract class CountingListener
  implements TraverseListener<File> {

    private int count;

    private final int finalCount;

    CountingListener(int finalCount) {
      this.finalCount = finalCount;
      assertTrue(finalCount > 0);
      assertTrue(finalCount % 2 == 0);
    }

    public void assertCompleted() {
      assertEquals(finalCount, count);
    }

    public int count() {
      return count;
    }

    protected void assertOrder(File expFile, File file) {
      assertOrder(expFile, file , count);
    }

    protected void assertOrder(File expFile, File file, int expCount) {
      assertEquals(expCount, count);
      ++count;
      assertEquals(expFile, file);
    }

  }

  private static class ZeroDepthListener extends CountingListener {

    final File root;



    ZeroDepthListener(File root) {
      super(2);
      this.root = root;
    }

    public void preorder(File file) {
      assertOrder(root, file, 0);
    }

    public void postorder(File file) {
      assertOrder(root, file, 1);
    }
  }
  
  
  File testDir;
  
  
  void initUnitTestDir(Object label) {
    testDir = getMethodOutputFilepath(label);
    assert testDir.mkdirs();
  }
  
  File unitTestDir() {
    return testDir;
  }




  /**
   * <pre>
   * testEmptyDir/
   * </pre>
   */
  @Test
  public void testEmptyDir() {
    initUnitTestDir(new Object() { });
    ZeroDepthListener listener = new ZeroDepthListener(unitTestDir());
    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.run();
    listener.assertCompleted();
  }

  /**
   * <pre>
   * testEmptyNondir/
   * </pre>
   */
  @Test
  public void testEmptyNondir() throws IOException {
    initUnitTestDir(new Object() { });
    File root = new File(unitTestDir(), "test-file");
    root.createNewFile();
    ZeroDepthListener listener = new ZeroDepthListener(root);
    FileSystemTraverser traverser = new FileSystemTraverser(root);
    traverser.setListener(listener);
    traverser.run();
    listener.assertCompleted();
  }




  static class AssertionListener extends CountingListener {

    private final Map<Integer, File> preorderMap
    = new HashMap<Integer, File>();

    private final Map<Integer, File> postorderMap
    = new HashMap<Integer, File>();



    public AssertionListener(int finalCount) {
      super(finalCount);
    }



    public Map<Integer, File> preorderMap() {
      return preorderMap;
    }

    public Map<Integer, File> postorderMap() {
      return postorderMap;
    }




    public void preorder(File file) {
      assertOrder(file, preorderMap);
    }

    public void postorder(File file) {
      assertOrder(file, postorderMap);
    }

    private void assertOrder(File file, Map<Integer, File> orderMap) {
      File expFile = orderMap.get(count());
      if (expFile == null)
        fail("illegal count: " + count());
      assertOrder(expFile, file);
    }

  }

  /**
   * <pre>
   * test03/
   *       |
   *        - a
   * </pre>
   */
  @Test
  public void test03() throws IOException {
    initUnitTestDir(new Object() { });
    final File a = new File(unitTestDir(), "a");
    a.createNewFile();

    CountingListener listener = new CountingListener(4) {

      public void preorder(File file) {
        File expFile;
        switch (count()) {
        case 0:
          expFile = unitTestDir();  break;
        case 1:
          expFile = a;        break;
        default:
          fail("illegal count: " + count());
          expFile = null; // (not reached)
        }
        assertOrder(expFile, file);
      }

      public void postorder(File file) {
        File expFile;
        switch (count()) {
        case 2:
          expFile = a;        break;
        case 3:
          expFile = unitTestDir();  break;
        default:
          fail("illegal count: " + count());
          expFile = null; // (not reached)
        }
        assertOrder(expFile, file);
      }

    };

    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.run();
    listener.assertCompleted();
  }

  /**
   * test03 redux using AssertionListener.
   * <pre>
   * test04/
   *       |
   *        - a
   * </pre>
   */
  @Test
  public void test04() throws IOException {
    initUnitTestDir(new Object() { });
    final File a = new File(unitTestDir(), "a");
    a.createNewFile();

    AssertionListener listener = new AssertionListener(4);
    listener.preorderMap().put(0, unitTestDir());
    listener.preorderMap().put(1, a);
    listener.postorderMap().put(2, a);
    listener.postorderMap().put(3, unitTestDir());

    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.run();
    listener.assertCompleted();
  }

  /**
   * <pre>
   * test05/
   *       |
   *        - a/
   * </pre>
   */
  @Test
  public void test05() throws IOException {
    initUnitTestDir(new Object() { });
    final File a = new File(unitTestDir(), "a");
    a.mkdir();

    AssertionListener listener = new AssertionListener(4);
    listener.preorderMap().put(0, unitTestDir());
    listener.preorderMap().put(1, a);
    listener.postorderMap().put(2, a);
    listener.postorderMap().put(3, unitTestDir());

    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.run();
    listener.assertCompleted();
  }

  /**
   * <pre>
   * test06/
   *       |
   *        - a/
   *       |
   *        - b
   * </pre>
   */
  @Test
  public void test06() throws IOException {
    initUnitTestDir(new Object() { });
    final File a = new File(unitTestDir(), "a");
    a.mkdir();
    final File b = new File(unitTestDir(), "b");
    b.createNewFile();

    AssertionListener listener = new AssertionListener(6);
    listener.preorderMap().put(0, unitTestDir());
    listener.preorderMap().put(1, a);
    listener.postorderMap().put(2, a);
    listener.preorderMap().put(3, b);
    listener.postorderMap().put(4, b);
    listener.postorderMap().put(5, unitTestDir());

    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.run();
    listener.assertCompleted();
  }

  /**
   * <pre>
   * test07/
   *       |
   *        - a/
   *           |
   *            - b
   * </pre>
   */
  @Test
  public void test07() throws IOException {
    initUnitTestDir(new Object() { });
    final File a = new File(unitTestDir(), "a");
    a.mkdir();
    final File b = new File(a, "b");
    b.createNewFile();

    AssertionListener listener = new AssertionListener(6);
    listener.preorderMap().put(0, unitTestDir());
    listener.preorderMap().put(1, a);
    listener.preorderMap().put(2, b);
    listener.postorderMap().put(3, b);
    listener.postorderMap().put(4, a);
    listener.postorderMap().put(5, unitTestDir());

    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.run();
    listener.assertCompleted();
  }

  /**
   * <pre>
   * test08/
   *       |
   *        - a/
   *       |   |
   *       |    - b
   *       |
   *        - c
   * </pre>
   */
  @Test
  public void test08() throws IOException {
    initUnitTestDir(new Object() { });
    final File a = new File(unitTestDir(), "a");
    a.mkdir();
    final File b = new File(a, "b");
    b.createNewFile();
    final File c = new File(unitTestDir(), "c");
    c.createNewFile();

    AssertionListener listener = new AssertionListener(8);
    listener.preorderMap().put(0, unitTestDir());
    listener.preorderMap().put(1, a);
    listener.preorderMap().put(2, b);
    listener.postorderMap().put(3, b);
    listener.postorderMap().put(4, a);
    listener.preorderMap().put(5, c);
    listener.postorderMap().put(6, c);
    listener.postorderMap().put(7, unitTestDir());

    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.run();
    listener.assertCompleted();
  }

  /**
   * <pre>
   * test09/
   *       |
   *        - c
   *       |
   *        - a/
   *       |   |
   *       |    - b
   * </pre>
   */
  @Test
  public void test09() throws IOException {
    initUnitTestDir(new Object() { });
    final File a = new File(unitTestDir(), "a");
    a.mkdir();
    final File b = new File(a, "b");
    b.createNewFile();
    final File c = new File(unitTestDir(), "c");
    c.createNewFile();

    AssertionListener listener = new AssertionListener(8);
    listener.preorderMap().put(0, unitTestDir());
    listener.preorderMap().put(1, c);
    listener.postorderMap().put(2, c);
    listener.preorderMap().put(3, a);
    listener.preorderMap().put(4, b);
    listener.postorderMap().put(5, b);
    listener.postorderMap().put(6, a);
    listener.postorderMap().put(7, unitTestDir());

    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.setSiblingOrder(DirectoryOrdering.FILE_FIRST);
    traverser.run();
    listener.assertCompleted();
  }

  /**
   * <pre>
   * test10/
   *       |
   *        - a/
   *       |   |
   *       |    - b
   *       |
   *        - c/
   *           |
   *            - d
   *           |
   *            - e
   * </pre>
   */
  @Test
  public void test10() throws IOException {
    initUnitTestDir(new Object() { });
    final File a = new File(unitTestDir(), "a");
    a.mkdir();
    final File b = new File(a, "b");
    b.createNewFile();
    final File c = new File(unitTestDir(), "c");
    c.mkdir();
    final File d = new File(c, "d");
    d.createNewFile();
    final File e = new File(c, "e");
    e.createNewFile();

    AssertionListener listener = new AssertionListener(12);
    listener.preorderMap().put(0, unitTestDir());
    listener.preorderMap().put(1, a);
    listener.preorderMap().put(2, b);
    listener.postorderMap().put(3, b);
    listener.postorderMap().put(4, a);
    listener.preorderMap().put(5, c);
    listener.preorderMap().put(6, d);
    listener.postorderMap().put(7, d);
    listener.preorderMap().put(8, e);
    listener.postorderMap().put(9, e);
    listener.postorderMap().put(10, c);
    listener.postorderMap().put(11, unitTestDir());

    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.run();
    listener.assertCompleted();
  }

  /**
   * <pre>
   * test11/
   *       |
   *        - a/
   *       |   |
   *       |    - b
   *       |
   *        - c/
   *           |
   *            - d
   *           |
   *            - e
   * </pre>
   */
  @Test
  public void test11() throws IOException {
    initUnitTestDir(new Object() { });
    final File a = new File(unitTestDir(), "a");
    a.mkdir();
    final File b = new File(a, "b");
    b.createNewFile();
    final File c = new File(unitTestDir(), "c");
    c.mkdir();
    final File d = new File(c, "d");
    d.createNewFile();
    final File e = new File(c, "e");
    e.createNewFile();

    AssertionListener listener = new AssertionListener(12);
    listener.preorderMap().put(0, unitTestDir());
    listener.preorderMap().put(1, a);
    listener.preorderMap().put(2, b);
    listener.postorderMap().put(3, b);
    listener.postorderMap().put(4, a);
    listener.preorderMap().put(5, c);
    listener.preorderMap().put(6, d);
    listener.postorderMap().put(7, d);
    listener.preorderMap().put(8, e);
    listener.postorderMap().put(9, e);
    listener.postorderMap().put(10, c);
    listener.postorderMap().put(11, unitTestDir());

    FileSystemTraverser traverser = new FileSystemTraverser(unitTestDir());
    traverser.setListener(listener);
    traverser.setSiblingOrder(DirectoryOrdering.FILE_FIRST);
    traverser.run();
    listener.assertCompleted();
  }

}
