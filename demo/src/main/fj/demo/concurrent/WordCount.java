package fj.demo.concurrent;

import static fj.Function.uncurryF2;
import static fj.Monoid.longAdditionMonoid;
import static fj.Monoid.monoid;
import static fj.Ord.stringOrd;
import static fj.control.parallel.ParModule.parModule;
import static fj.data.List.list;
import static fj.data.List.nil;
import static fj.function.Integers.add;
import static java.util.concurrent.Executors.newFixedThreadPool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import fj.Effect;
import fj.F;
import fj.F2;
import fj.Monoid;
import fj.Ord;
import fj.P;
import fj.P1;
import fj.P2;
import fj.Unit;
import fj.control.parallel.ParModule;
import fj.control.parallel.Promise;
import fj.control.parallel.Strategy;
import fj.data.IO;
import fj.data.Iteratee;
import fj.data.Iteratee.Input;
import fj.data.Iteratee.IterV;
import fj.data.LazyString;
import fj.data.List;
import fj.data.Option;
import fj.data.Stream;
import fj.data.TreeMap;
import fj.function.Characters;

/**
 * Reads words and their counts from files ({@link #getWordsAndCountsFromFiles(List)} in a single thread
 * and {@link #getWordsAndCountsFromFilesInParallel(List, int)} in multiple threads). The files are created
 * initially and populated with some sample content.
 * 
 * @author Martin Grotzke
 */
public class WordCount {
  
  private static final F2<Integer, Integer, Integer> integersAdd = uncurryF2(add);

  // reads the given files and returns their content as char stream
  private static final F<String, LazyString> readFileToLazyString = new F<String, LazyString>() {
    @Override
    public LazyString f(final String fileName) {
      try {
        // return LazyString.str(readFileToString(new File(fileName)));
        Stream<Character> chars = IO.enumFileCharChunks(new File(fileName), IO.streamFromCharChunks()).run().run();
        return LazyString.fromStream(chars);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  };
  
  // counts words from the given char stream
  private static final F<LazyString, Long> countWordsFromLazyString = new F<LazyString, Long>() {
    @Override
    public Long f(final LazyString document) {
      return (long) document.split(Characters.isWhitespace).length();
    }
  };
  
  // map of words to their counts (occurrences)
  private static final F2<TreeMap<String, Integer>, LazyString, TreeMap<String, Integer>> wordsAndCounts =
        new F2<TreeMap<String, Integer>, LazyString, TreeMap<String, Integer>>() {
    @Override
    public TreeMap<String, Integer> f(final TreeMap<String, Integer> map,
        final LazyString word) {
      return map.update(word.toString(), add.f(1), Integer.valueOf(1));
    }
  };
  
  private static final F<LazyString, List<LazyString>> wordsFromLazyString = new F<LazyString, List<LazyString>>() {
    @Override
    public List<LazyString> f(final LazyString a) {
      return a.split(Characters.isWhitespace).toList();
    }
  };

  private static final F<String, TreeMap<String, Integer>> fileNameToWordsAndCounts = new F<String, TreeMap<String, Integer>>() {
    @Override
    public TreeMap<String, Integer> f(final String a) {
      return wordsFromLazyString.f(readFileToLazyString.f(a))
          .foldLeft(wordsAndCounts, TreeMap.<String, Integer> empty(stringOrd));
    }
  };
  
  private static final F<String, TreeMap<String, Integer>> fileNameToWordsAndCountsWithIteratee = new F<String, TreeMap<String, Integer>>() {
    @Override
    public TreeMap<String, Integer> f(final String fileName) {
      try {
        return IO.enumFileChars(new File(fileName), wordCountsFromChars()).run().run();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  };

  /** An iteratee that consumes chars and calculates word counts */
  public static final <E> IterV<Character, TreeMap<String, Integer>> wordCountsFromChars() {
    final F<P2<StringBuilder,TreeMap<String, Integer>>, F<Input<Character>, IterV<Character, TreeMap<String, Integer>>>> step =
      new F<P2<StringBuilder,TreeMap<String, Integer>>, F<Input<Character>, IterV<Character, TreeMap<String, Integer>>>>() {
        final F<P2<StringBuilder,TreeMap<String, Integer>>, F<Input<Character>, IterV<Character, TreeMap<String, Integer>>>> step = this;

        @Override
        public F<Input<Character>, IterV<Character, TreeMap<String, Integer>>> f(final P2<StringBuilder,TreeMap<String, Integer>> acc) {
          final P1<IterV<Character, TreeMap<String, Integer>>> empty =
            new P1<IterV<Character, TreeMap<String, Integer>>>() {
              @Override
              public IterV<Character, TreeMap<String, Integer>> _1() {
                return IterV.cont(step.f(acc));
              }
            };
          final P1<F<Character, IterV<Character, TreeMap<String, Integer>>>> el =
            new P1<F<Character, IterV<Character, TreeMap<String, Integer>>>>() {
              @Override
              public F<Character, IterV<Character, TreeMap<String, Integer>>> _1() {
                return new F<Character, Iteratee.IterV<Character, TreeMap<String, Integer>>>() {
                  @Override
                  public IterV<Character, TreeMap<String, Integer>> f(final Character e) {
                    if(Character.isWhitespace(e.charValue())) {
                      final StringBuilder sb = acc._1();
                      if(sb.length() > 0) {
                        final TreeMap<String, Integer> map = acc._2().update(sb.toString(), add.f(1), Integer.valueOf(1));
                        return IterV.cont(step.f(P.p(new StringBuilder(), map)));
                      }
                      else {
                        // another whitespace char, no word to push to the map
                        return IterV.cont(step.f(acc));
                      }
                    }
                    else {
                      acc._1().append(e);
                      return IterV.cont(step.f(acc));
                    }
                  }
                };
              }
            };
          final P1<IterV<Character, TreeMap<String, Integer>>> eof =
            new P1<IterV<Character, TreeMap<String, Integer>>>() {
              @Override
              public IterV<Character, TreeMap<String, Integer>> _1() {
                final StringBuilder sb = acc._1();
                if(sb.length() > 0) {
                  final TreeMap<String, Integer> map = acc._2().update(sb.toString(), add.f(1), Integer.valueOf(1));
                  return IterV.done(map, Input.<Character>eof());
                }
                return IterV.done(acc._2(), Input.<Character>eof());
              }
            };
          return new F<Input<Character>, IterV<Character, TreeMap<String, Integer>>>() {
            @Override
            public IterV<Character, TreeMap<String, Integer>> f(final Input<Character> s) {
              return s.apply(empty, el, eof);
            }
          };
        }
      };
    return IterV.cont(step.f(P.p(new StringBuilder(), TreeMap.<String, Integer> empty(stringOrd))));
  }
  
  public static void main(String[] args) throws IOException {

    // setup
    int numFiles = 100;
    int numSharedWords = 200;

    final P2<List<String>, Map<String, Integer>> result = writeSampleFiles(numFiles, numSharedWords);
    final List<String> fileNames = result._1();
    final Map<String, Integer> expectedWordsAndCounts = result._2();

    // get word counts sequentially / single threaded
    long start = System.currentTimeMillis();
    TreeMap<String, Integer> wordsAndCountsFromFiles = getWordsAndCountsFromFiles(fileNames);
    System.out.println("Getting word counts in 1 thread took " + (System.currentTimeMillis() - start) + " ms.");
    assertTrue(wordsAndCountsFromFiles != null);
    assertTrue(wordsAndCountsFromFiles.size() == numFiles + numSharedWords);
    assertTrue(wordsAndCountsFromFiles.toMutableMap().equals(expectedWordsAndCounts));

    // get word counts sequentially / single threaded \w iteratee
    start = System.currentTimeMillis();
    wordsAndCountsFromFiles = getWordsAndCountsFromFilesWithIteratee(fileNames);
    System.out.println("Getting word counts in 1 thread using iteratee took " + (System.currentTimeMillis() - start) + " ms.");
    assertTrue(wordsAndCountsFromFiles != null);
    assertEquals(wordsAndCountsFromFiles.size(), numFiles + numSharedWords);
    assertEquals(wordsAndCountsFromFiles.toMutableMap(), expectedWordsAndCounts);

    start = System.currentTimeMillis();
    wordsAndCountsFromFiles = getWordsAndCountsFromFilesInParallel(fileNames, fileNameToWordsAndCounts, 8);
    System.out.println("Getting word counts in 8 threads took " + (System.currentTimeMillis() - start) + " ms.");
    assertTrue(wordsAndCountsFromFiles != null);
    assertEquals(wordsAndCountsFromFiles.size(), numFiles + numSharedWords);
    assertEquals(wordsAndCountsFromFiles.toMutableMap(), expectedWordsAndCounts);

    start = System.currentTimeMillis();
    wordsAndCountsFromFiles = getWordsAndCountsFromFilesInParallel(fileNames, fileNameToWordsAndCountsWithIteratee, 8);
    System.out.println("Getting word counts in 8 threads with iteratee took " + (System.currentTimeMillis() - start) + " ms.");
    assertTrue(wordsAndCountsFromFiles != null);
    assertEquals(wordsAndCountsFromFiles.size(), numFiles + numSharedWords);
    assertEquals(wordsAndCountsFromFiles.toMutableMap(), expectedWordsAndCounts);
    
    // we have tmpfiles, but still want to be sure not to leave rubbish
    fileNames.foreach(new Effect<String>() {
      @Override
      public void e(final String a) {
        new File(a).delete();
      }});
  }

  @SuppressWarnings("unused")
  private static void print(TreeMap<String, Integer> wordsAndCountsFromFiles) {
    for(final Map.Entry<String, Integer> entry : wordsAndCountsFromFiles.toMutableMap().entrySet()) {
      System.out.println("Have " + entry.getKey() + ": " + entry.getValue());
    }
  }

  private static P2<List<String>, Map<String, Integer>> writeSampleFiles(
      int numFiles, int numSharedWords) throws IOException {
    final Map<String, Integer> expectedWordsAndCounts = new HashMap<String, Integer>();
    List<String> fileNames = nil();
    for(int i = 0; i < numFiles; i++) {
      final File file = File.createTempFile("wordcount-"+ i + "-", ".txt");
      final BufferedWriter writer = new BufferedWriter(new FileWriter(file));
      writer.write("File" + i + "\n");
      expectedWordsAndCounts.put("File" + i, 1);
      for(int j = 0; j < numSharedWords; j++) {
        writer.write("\nsomeword" + j);
        expectedWordsAndCounts.put("someword" + j, numFiles);
      }
      writer.close();
      fileNames = fileNames.cons(file.getAbsolutePath());
    }
    return P.p(fileNames, expectedWordsAndCounts);
  }

  // Read documents and count words of documents in parallel
  private static Promise<Long> countWordsFromFiles(final List<String> fileNames,
      final ParModule m) {
    return m.parFoldMap(fileNames, readFileToLazyString.andThen(countWordsFromLazyString), longAdditionMonoid);
  }
  
  // Read documents and extract words and word counts of documents
  public static TreeMap<String, Integer> getWordsAndCountsFromFiles(final List<String> fileNames) {
    return fileNames.map(readFileToLazyString).bind(wordsFromLazyString)
        .foldLeft(wordsAndCounts, TreeMap.<String, Integer> empty(stringOrd));
  }
  
  public static TreeMap<String, Integer> getWordsAndCountsFromFilesWithIteratee(final List<String> fileNames) {
  return fileNames.map(fileNameToWordsAndCountsWithIteratee).foldLeft1(new F2<TreeMap<String, Integer>, TreeMap<String, Integer>, TreeMap<String, Integer>>() {
    @Override
    public TreeMap<String, Integer> f(TreeMap<String, Integer> a, TreeMap<String, Integer> b) {
      return plus(a, b, integersAdd, stringOrd);
    }});
}
  
  public static TreeMap<String, Integer> getWordsAndCountsFromFilesInParallel(
      final List<String> fileNames, final F<String, TreeMap<String, Integer>> fileNameToWordsAndCounts, int numThreads) {
    final ExecutorService pool = newFixedThreadPool(numThreads);
    final ParModule m = parModule(Strategy.<Unit> executorStrategy(pool));

    // Long wordCount = countWords(fileNames.map(readFile), m).claim();    
    final TreeMap<String, Integer> result = getWordsAndCountsFromFiles(fileNames, fileNameToWordsAndCounts, m).claim();

    pool.shutdown();

    return result;
  }
  
  // Read documents and extract words and word counts of documents
  public static Promise<TreeMap<String, Integer>> getWordsAndCountsFromFiles(
      final List<String> fileNames, final F<String, TreeMap<String, Integer>> fileNameToWordsAndCounts, final ParModule m) {
    final F<TreeMap<String, Integer>, F<TreeMap<String, Integer>, TreeMap<String, Integer>>> treeMapSum =
        new F<TreeMap<String, Integer>, F<TreeMap<String, Integer>, TreeMap<String, Integer>>>() {
      @Override
      public F<TreeMap<String, Integer>, TreeMap<String, Integer>> f(
          final TreeMap<String, Integer> a) {
        return new F<TreeMap<String, Integer>, TreeMap<String, Integer>>() {

          @Override
          public TreeMap<String, Integer> f(final TreeMap<String, Integer> b) {
            return plus(a, b, integersAdd, stringOrd);
          }
          
        };
      }
      
    };
    final Monoid<TreeMap<String, Integer>> monoid = monoid(treeMapSum,
        TreeMap.<String, Integer> empty(stringOrd));
    return m.parFoldMap(fileNames, fileNameToWordsAndCounts, monoid);
  }
  
  private static <K, V> TreeMap<K, V> plus(final TreeMap<K, V> a,
      final TreeMap<K, V> b,
      final F2<V, V, V> update,
      final Ord<K> ord) {
    if(a.isEmpty()) {
      return b;
    }
    else if (b.isEmpty()) {
      return a;
    }
    final Map<K, V> ma = a.toMutableMap();
    // Update all entries in a by adding the values of matching keys from b
    for(final Entry<K, V> entry : ma.entrySet()) {
      final Option<V> value = b.get(entry.getKey());
      if(value.isSome()) {
        entry.setValue(update.f(entry.getValue(), value.some()));
      }
    }
    // Add all entries from b that are not already in a
    for(final Entry<K, V> entry : b.toMutableMap().entrySet()) {
      if(!ma.containsKey(entry.getKey())) {
        ma.put(entry.getKey(), entry.getValue());
      }
    }
    return TreeMap.fromMutableMap(ord, ma);
  }

  // Main program does the requisite IO gymnastics
  public static Long countWords(final String... fileNames) {
    return countWords(list(fileNames));
  }

  // Main program does the requisite IO gymnastics
  public static Long countWords(final List<String> fileNames) {

    final ExecutorService pool = newFixedThreadPool(1);
    final ParModule m = parModule(Strategy.<Unit> executorStrategy(pool));

    // Long wordCount = countWords(fileNames.map(readFile), m).claim();
    final Long wordCount = countWordsFromFiles(fileNames, m).claim();
    System.out.println("Word Count: " + wordCount);

    pool.shutdown();

    return wordCount;
  }
  
  @SuppressWarnings("unused")
  private static String readFileToString(File file) throws IOException {
        Reader reader = null;
        try {
            reader = new FileReader(file);
            final Writer sw = new StringWriter((int)file.length());
            copy(reader, sw);
            return sw.toString();
        } finally {
          reader.close();
        }
    }

  private static void copy(Reader reader, Writer writer) throws IOException {
    char[] buffer = new char[1024 * 4];
    int n = 0;
    while (-1 != (n = reader.read(buffer))) {
        writer.write(buffer, 0, n);
    }
  }
  
  static void assertTrue(boolean condition) {
    if (!condition) {
      throw new AssertionError();
    }
  }
  
  static void assertEquals(Object actual, Object expected) {
    if (!expected.equals(actual)) {
      throw new IllegalArgumentException("Not equals. Expected: " + expected + ", actual: " + actual);
    }
  }

}
