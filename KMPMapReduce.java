import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.naming.Context;

public class KMPMapReduce {

    public static class KMPMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String pattern;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pattern = context.getConfiguration().get("pattern");
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();

            initialOffset = fileSplit.getoffset
            // read each line in file splits
			Scanner reader = new Scanner(value.toString());
			// tokenize each word with a space
			while (reader.hasNextLine()) {
                String line = reader.nextLine();
                    // perform kmp on line, add offset to each index
                    
                    // update offset using how many characters in line
            }
            List<Integer> occurrences = kmpSearch(text);

            if (!occurrences.isEmpty()) {
                StringBuilder indices = new StringBuilder();
                for (int index : occurrences) {
                    indices.append(index).append(",");
                }
                context.write(new Text(filename), new Text(indices.toString()));
            }
        }

        private List<Integer> kmpSearch(String text) {
            List<Integer> occurrences = new ArrayList<>();

            int m = pattern.length();
            int n = text.length();

            int[] lps = computeLPSArray(pattern);

            int i = 0;
            int j = 0;

            while (i < n) {
                if (pattern.charAt(j) == text.charAt(i)) {
                    i++;
                    j++;
                }
                if (j == m) {
                    occurrences.add(i - j + offset);
                    j = lps[j - 1];
                } else if (i < n && pattern.charAt(j) != text.charAt(i)) {
                    if (j != 0)
                        j = lps[j - 1];
                    else
                        i++;
                }
            }
            return occurrences;
        }

        private int[] computeLPSArray(String pattern) {
            int m = pattern.length();
            int[] lps = new int[m];
            int len = 0;
            int i = 1;
            lps[0] = 0;

            while (i < m) {
                if (pattern.charAt(i) == pattern.charAt(len)) {
                    len++;
                    lps[i] = len;
                    i++;
                } else {
                    if (len != 0)
                        len = lps[len - 1];
                    else {
                        lps[i] = len;
                        i++;
                    }
                }
            }
            return lps;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: KMPMapReduce <input_path> <output_path> <pattern>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("pattern", args[2]);

        Job job = Job.getInstance(conf, "KMP MapReduce");
        job.setJarByClass(KMPMapReduce.class);
        job.setMapperClass(KMPMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
