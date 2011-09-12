package fj.demo.concurrent;

import static fj.Function.uncurryF2;
import static fj.Monoid.longAdditionMonoid;
import static fj.Monoid.monoid;
import static fj.Ord.stringOrd;
import static fj.control.parallel.ParModule.parModule;
import static fj.data.LazyString.str;
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
import fj.P2;
import fj.Unit;
import fj.control.parallel.ParModule;
import fj.control.parallel.Promise;
import fj.control.parallel.Strategy;
import fj.data.LazyString;
import fj.data.List;
import fj.data.Option;
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
				return str(readFileToString(new File(fileName)));
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
//    	for(final Map.Entry<String, Integer> entry : wordsAndCountsFromFiles.toMutableMap().entrySet()) {
//    		System.out.println("Have " + entry.getKey() + ": " + entry.getValue());
//    	}
		assert wordsAndCountsFromFiles != null;
    	assert wordsAndCountsFromFiles.size() == numFiles + numSharedWords;
    	assert wordsAndCountsFromFiles.toMutableMap().equals(expectedWordsAndCounts);

		start = System.currentTimeMillis();
    	wordsAndCountsFromFiles = getWordsAndCountsFromFilesInParallel(fileNames, 8);
    	System.out.println("Getting word counts in 8 threads took " + (System.currentTimeMillis() - start) + " ms.");
//    	for(final Map.Entry<String, Integer> entry : wordsAndCountsFromFiles.toMutableMap().entrySet()) {
//    		System.out.println("Have " + entry.getKey() + ": " + entry.getValue());
//    	}
		assert wordsAndCountsFromFiles != null;
    	assert wordsAndCountsFromFiles.size() == numFiles + numSharedWords;
    	assert wordsAndCountsFromFiles.toMutableMap().equals(expectedWordsAndCounts);
		
		// we have tmpfiles, but still want to be sure not to leave rubbish
		fileNames.foreach(new Effect<String>() {
			@Override
			public void e(final String a) {
				new File(a).delete();
			}});
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
	
	public static TreeMap<String, Integer> getWordsAndCountsFromFilesInParallel(
			final List<String> fileNames, int numThreads) {
		final ExecutorService pool = newFixedThreadPool(numThreads);
		final ParModule m = parModule(Strategy.<Unit> executorStrategy(pool));

		// Long wordCount = countWords(fileNames.map(readFile), m).claim();
		final TreeMap<String, Integer> result = getWordsAndCountsFromFiles(fileNames, m).claim();

		pool.shutdown();

		return result;
	}
	
	// Read documents and extract words and word counts of documents
	public static Promise<TreeMap<String, Integer>> getWordsAndCountsFromFiles(
			final List<String> fileNames, final ParModule m) {
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
		final F<String, TreeMap<String, Integer>> fileNameToWordsAndCounts =
				new F<String, TreeMap<String, Integer>>() {
			@Override
			public TreeMap<String, Integer> f(final String a) {
				return wordsFromLazyString.f(readFileToLazyString.f(a))
						.foldLeft(wordsAndCounts, TreeMap.<String, Integer> empty(stringOrd));
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

}
