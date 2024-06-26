package tyut.hjy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class Merge {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            text = value;
            context.write(text, new Text(""));
        }
    }

    public static class Reduce extends Mapper<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://home.firegoldfish.com:9000");
        String[] otherArgs = new String[]{"hdfs://home.firegoldfish.com:9000/MapReduce", "hdfs://home.firegoldfish.com:9000/output"};
        if(otherArgs.length != 2){
            System.err.println("Usage: Merge <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Merge");
        job.setJarByClass(Merge.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

