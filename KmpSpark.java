import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import java.util.Iterator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class KmpSpark {

    public static void main(String[] args) {

        // Initialize Spark context and load text file RDD
        SparkConf conf = new SparkConf().setAppName("KmpSpark");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Start the timer
        long startTime = System.currentTimeMillis();
        // Load text files into a PairRDD, where key is the file path and value is the file content
        JavaPairRDD<String, String> textRDD = jsc.wholeTextFiles(args[0] + "/*");

        // Get the search pattern from command-line arguments
        final String pattern = args[2];


        // Per file KMP search function 
        PairFunction<Tuple2<String,String>, String, List<Integer>> func =
                new PairFunction<Tuple2<String,String>, String, List<Integer>>() {

                    @Override
                    public Tuple2<String, List<Integer>> call(Tuple2<String, String> tuple) {

                        String filePath = tuple._1();
                        // Parse file path to get just file name
                        Path p = Paths.get(filePath);
                        String fileName = p.getFileName().toString();
                        
                        // Extract file content
                        String text = tuple._2();

                        // Perform KMP search on the file content
                        List<Integer> indices = new ArrayList<>();

                        Iterator<List<Integer>> indexIter = new KMPSearch(pattern).call(text);
                        while(indexIter.hasNext()) {
                            indices.addAll(indexIter.next());
                        }
                        // Return Tuple2 with file name and corresponding indices
                        return new Tuple2<>(fileName, indices);
                    }
                };


        // Execute per file search on RDD 
        JavaPairRDD<String, List<Integer>> resultsRDD =
                textRDD.mapToPair(func);
        // Concatenate output to a single file and save results
        resultsRDD = resultsRDD.coalesce(1);
        resultsRDD.saveAsTextFile(args[1]);
        // Print the total execution time
        System.out.println("Time = " + (System.currentTimeMillis() - startTime));
        jsc.stop();
    }

    // KMP Search function 
    public static class KMPSearch implements FlatMapFunction<String, List<Integer>> {

        private final String pattern;

        public KMPSearch(String pat) {
            this.pattern = pat;
        }

        @Override
        public Iterator<List<Integer>> call(String text) {
            
            // List to store matching indices
            List<Integer> matchIndices = new ArrayList<>();

            // Length of the pattern
            int M = pattern.length();

            // Compute the Longest Prefix Suffix (LPS) array
            int[] lps = computeLPSArray(this.pattern);

            // KMP search algorithm
            int i = 0, j = 0;
            while (i < text.length()) {
                if (pattern.charAt(j) == text.charAt(i)) {
                    j++;
                    i++;
                }

                if (j == M) {
                    // Match found, add the starting index to the list
                    matchIndices.add(i - j);
                    j = lps[j - 1];
                } else if (i < text.length() && pattern.charAt(j) != text.charAt(i)) {
                    if (j != 0)
                        j = lps[j - 1];
                    else
                        i = i + 1;
                }
            }
            // Return a single-element iterator containing the list of matching indices
            return Collections.singletonList(matchIndices).iterator();
        }



    }

    // Helper method to compute the Longest Prefix Suffix (LPS) array
        public static int[] computeLPSArray(String pattern) {
            int len = 0;
            int i = 1;
            int[] lps = new int[pattern.length()];

            lps[0] = 0;

            while (i < pattern.length()) {
                if (pattern.charAt(i) == pattern.charAt(len)) {
                    len++;
                    lps[i] = len;
                    i++;
                }
                else {
                    if (len != 0) {
                        len = lps[len - 1];
                    }
                    else {
                        lps[i] = 0;
                        i++;
                    }
                }
            }
            return lps;
        }
    }




