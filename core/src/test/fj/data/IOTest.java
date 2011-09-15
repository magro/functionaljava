/**
 * 
 */
package fj.data;

import static fj.Equal.charEqual;
import static fj.Equal.streamEqual;
import static fj.data.IO.enumFileLines;
import static fj.data.List.list;
import static fj.data.Stream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;

import org.testng.annotations.Test;

import fj.F;
import fj.F2;
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
    assertEquals(enumFileLines(f, IterV.<String>length()).run().run(), Integer.valueOf(3));
  }

  @Test
  public void testEnumFileHead() throws IOException {
    final File f = writeTmpFile("testEnumFileHead", "foo\nbar\nbaz");
    final Option<String> head = enumFileLines(f, IterV.<String>head()).run().run();
    assertTrue(head.isSome());
    assertEquals(head.some(), "foo");
  }

  @Test
  public void testEnumFileDrop1() throws IOException {
    final File f = writeTmpFile("testEnumFileDrop1", "foo\nbar\nbaz");
    final IterV<String, Option<String>> head = IterV.<String>head();
    final Option<String> dropped1 = enumFileLines(f, IterV.<String>drop(1).bind(Function.<Unit, IterV<String, Option<String>>>constant(head))).run().run();
    assertTrue(dropped1.isSome());
    assertEquals(dropped1.some(), "bar");
  }

  @Test
  public void testEnumFileDrop2() throws IOException {
    final File f = writeTmpFile("testEnumFileDrop2", "foo\nbar\nbaz");
    final IterV<String, Option<String>> head = IterV.<String>head();
    final Option<String> dropped2 = enumFileLines(f, IterV.<String>drop(2).bind(Function.<Unit, IterV<String, Option<String>>>constant(head))).run().run();
    assertTrue(dropped2.isSome());
    assertEquals(dropped2.some(), "baz");
  }

  @Test
  public void testEnumFileList() throws IOException {
    final File f = writeTmpFile("testEnumFileRead", "foo\nbar\nbaz");
    final List<String> lines = enumFileLines(f, IterV.<String> list()).run().run();
    assertFalse(lines.isEmpty());
    assertEquals(lines.toCollection(), list("foo", "bar", "baz").toCollection());
  }
  
  @Test
  public void testStreamFromChars() {
    assertTrue(Stream.fromChars(new char[0]).isEmpty());
    assertEquals(Stream.fromChars(new char[]{'f', 'o', 'o'}).length(), 3);
  }
  
  @Test
  public void testStreamFromSomeMoreChars() {
    final Stream<String> content = range(0, 1000).map(new F<Integer, String>() {
      @Override
      public String f(Integer a) {
        return a.toString();
      }});
    
    long start = System.currentTimeMillis();
    Stream<Character> contentAsCharStream = content.foldLeft(new F2<Stream<Character>, String, Stream<Character>>() {
      @Override
      public Stream<Character> f(Stream<Character> a, String b) {
        return a.append(Stream.fromString(b));
      }}, Stream.<Character> nil());
    System.out.println("Creating char stream from string stream took " + (System.currentTimeMillis() - start) + " ms.");
    
    start = System.currentTimeMillis();
    int length = contentAsCharStream.length();
    System.out.println("Calculating length ("+length+") from char stream took " + (System.currentTimeMillis() - start) + " ms.");
    
    
    
//    Arrays.fill(chars, '0');
//    String s = String.valueOf(chars);
//    long start = System.currentTimeMillis();
//    Stream<Character> stream = Stream.fromString(s);
//    System.out.println("Creating stream from string took " + (System.currentTimeMillis() - start) + " ms.");
//    
//    start = System.currentTimeMillis();
//    Stream<Integer> ints = stream.map(new F<Character,Integer>(){
//      @Override
//      public Integer f(Character a) {
//        return Integer.valueOf(a.charValue());
//      }});
//    int length = ints.length();
//    System.out.println("Creating int stream (length "+length+") from char stream took " + (System.currentTimeMillis() - start) + " ms.");
    
  }

  // This takes some time and with more content (1.000.000 words) it doesn't complete
  @Test
  public void testEnumFileFromSomeMoreChars() throws IOException {
    
    final Stream<String> content = range(0, 1000).map(new F<Integer, String>() {
      @Override
      public String f(Integer a) {
        return a.toString();
      }});
    
    final File f = writeTmpFile("testEnumFileFromSomeMoreChars", content );
    System.out.println("Created file "+ f.getAbsolutePath() +" with " + (f.length() / 1024) + " kB ("+f.length()+")");
    
    long start = System.currentTimeMillis();
    final Stream<Character> charStream = IO.enumFileChunks(f, IO.streamFromChars()).run().run();
    System.out.println("Reading charStream from file took " + (System.currentTimeMillis() - start) + " ms.");
    
    start = System.currentTimeMillis();
    int length = charStream.length();
    System.out.println("Computing charStream length ("+ length +") took " + (System.currentTimeMillis() - start) + " ms.");
    
    //
    Stream<Character> contentAsCharStream = content.foldLeft(new F2<Stream<Character>, String, Stream<Character>>() {
      @Override
      public Stream<Character> f(Stream<Character> a, String b) {
        return a.append(Stream.fromChars(b.toCharArray()));
      }}, Stream.<Character> nil());
    assertTrue(streamEqual(charEqual).eq(contentAsCharStream, charStream));
//    final LazyString lines = LazyString.fromStream(charStream);
//    assertFalse(lines.isEmpty());
//    assertEquals(lines.lines().map(LazyString.toString).toCollection(), list("foo", "bar", "baz").toCollection());
  }

  @Test
  public void testEnumFileFromChars() throws IOException {
    final File f = writeTmpFile("testEnumFileFromChars", "foo\nbar\nbaz");
    final Stream<Character> charStream = IO.enumFileChunks(f, IO.streamFromChars()).run().run();
    final LazyString lines = LazyString.fromStream(charStream);
    assertFalse(lines.isEmpty());
    assertEquals(lines.lines().map(LazyString.toString).toCollection(), list("foo", "bar", "baz").toCollection());
  }

  @Test
  public void testEnumFileFromCharStreamFromLines() throws IOException {
    final File f = writeTmpFile("testEnumFileAsLazyString", "foo\nbar\nbaz");
    final Stream<Character> charStream = IO.enumFileLines(f, IO.charStreamFromLines()).run().run();
    final LazyString lines = LazyString.fromStream(charStream);
    assertFalse(lines.isEmpty());
    assertEquals(lines.lines().map(LazyString.toString).toCollection(), list("foo", "bar", "baz").toCollection());
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

  private File writeTmpFile(String name, Stream<String> content) throws IOException {
    final File result = File.createTempFile(name, ".tmp");
    final Writer writer = new BufferedWriter(new FileWriter(result));
    try {
      for(String c : content)
        writer.write(c);
    } finally {
      writer.close();
    }
    return result;
  }

  
}
