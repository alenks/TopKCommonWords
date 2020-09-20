import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class topKCW {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{
        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text fileName = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program
            String fName = ((FileSplit) context.getInputSplit()).getPath().getName();
            fileName.set(fName);
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, fileName);
            }
        }
    }



    public static class IntSumReducer<K extends WritableComparable, V extends Writable>
            extends Reducer<Text,Text,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map<String, Integer> WordFileCtr = new HashMap<String, Integer>();
            for (Text fname : values) {
                String valFile = fname.toString();
                int count = WordFileCtr.getOrDefault(valFile, 0);
                WordFileCtr.put(valFile, count+1);
            }
            if (WordFileCtr.size() == 2) {
                int min_sum = Collections.min(WordFileCtr.values());
                result.set(min_sum);
                context.write(key, result);
            }
        }
    }

    /*
        public static class SFMapper
            extends Mapper<Object, Text, Text, Text>{
        private TreeMap<Integer, String> tmap = new TreeMap<Integer, String>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String word = tokens[0];
            Integer count = -1 * Integer.parseInt(tokens[1]);
            tmap.put(count, word);
            if (tmap.size() > 20) {
                tmap.remove(tmap.firstKey());
            }
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : tmap.entrySet()) {
                Integer count = entry.getKey();
                String word = entry.getValue();
                context.write(new Text(word), new Text(Integer.toString(count)));
            }
        }
    }

    public static class SFReducer
            extends Reducer<Text,Text,Text,Text> {
        private TreeMap<Integer, String> tmap2 = new TreeMap<Integer, String>();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String word = key.toString();
            Integer count = 0;

            for (Text val : values)
            {
                count = Integer.parseInt(val.toString());
            }
            tmap2.put(count, word);
            if (tmap2.size() > 20)
            {
                tmap2.remove(tmap2.firstKey());
            }
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : tmap2.entrySet()) {
                Integer count = -1 * entry.getKey();
                String name = entry.getValue();
                context.write(new Text(Integer.toString(count)), new Text(name));
            }
        }
    }

    */


    public static class SFMapper
            extends Mapper<Object, Text, IntWritable, Text>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String word = tokens[0];
            int count = -1 * Integer.parseInt(tokens[1]);
            context.write(new IntWritable(count), new Text(word));
        }
    }

    public static class SFReducer
            extends Reducer<IntWritable,Text,Text,Text> {
        int ctr = 0;
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String word;
            int count = -1 * key.get();

            for (Text val : values) {
                word = val.toString();
                if (ctr < 20) {
                    context.write(new Text(Integer.toString(count)), new Text(word));
                }
                ++ctr;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String tempDir =  "tmp";

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "top k word count");
        job1.setJarByClass(topKCW.class);
        job1.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path(tempDir));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "sort and filter");
        job2.setJarByClass(topKCW.class);
        job2.setMapperClass(SFMapper.class);
        job2.setReducerClass(SFReducer.class);
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(tempDir));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}
