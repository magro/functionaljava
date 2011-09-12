/**
 * 
 */
package fj.data;

import static fj.data.IO.enumFile;
import static fj.data.List.list;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.testng.annotations.Test;

import fj.Function;
import fj.Unit;
import fj.data.Iteratee.IterV;

/**
 * @author Martin Grotzke
 *
 */
public class IOTest {

  @Test
  public void testEnumFileLength() throws IOException {
    final File f = writeTmpFile("testEnumFileLength", "foo\nbar\nbaz");
    assertEquals(enumFile(f, IterV.<String>length()).run().run(), Integer.valueOf(3));
  }

  @Test
  public void testEnumFileHead() throws IOException {
    final File f = writeTmpFile("testEnumFileHead", "foo\nbar\nbaz");
    final Option<String> head = enumFile(f, IterV.<String>head()).run().run();
    assertTrue(head.isSome());
    assertEquals(head.some(), "foo");
  }

  @Test
  public void testEnumFileDrop1() throws IOException {
    final File f = writeTmpFile("testEnumFileDrop1", "foo\nbar\nbaz");
    final IterV<String, Option<String>> head = IterV.<String>head();
    final Option<String> dropped1 = enumFile(f, IterV.<String>drop(1).bind(Function.<Unit, IterV<String, Option<String>>>constant(head))).run().run();
    assertTrue(dropped1.isSome());
    assertEquals(dropped1.some(), "bar");
  }

  @Test
  public void testEnumFileDrop2() throws IOException {
    final File f = writeTmpFile("testEnumFileDrop2", "foo\nbar\nbaz");
    final IterV<String, Option<String>> head = IterV.<String>head();
    final Option<String> dropped2 = enumFile(f, IterV.<String>drop(2).bind(Function.<Unit, IterV<String, Option<String>>>constant(head))).run().run();
    assertTrue(dropped2.isSome());
    assertEquals(dropped2.some(), "baz");
  }

  @Test
  public void testEnumFileList() throws IOException {
    final File f = writeTmpFile("testEnumFileRead", "foo\nbar\nbaz");
    final List<String> lines = enumFile(f, IterV.<String> list()).run().run();
    assertFalse(lines.isEmpty());
    assertEquals(lines.toCollection(), list("foo", "bar", "baz").toCollection());
  }

  private File writeTmpFile(String name, String content) throws IOException {
    final File result = File.createTempFile(name, ".tmp");
    final FileWriter writer = new FileWriter(result);
    try {
      writer.write(content);
    } finally {
      writer.close();
    }
    return result;
  }

  
}
