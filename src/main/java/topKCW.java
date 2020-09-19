import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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



    public static class IntSumReducer
            extends Reducer<Text,Text,IntWritable,Text> {
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
                context.write(result, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top k word count");
        job.setJarByClass(topKCW.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
