package org.ponyKnight.mapre.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class LongestLengthJob {
    public static JobConf createJob(Configuration config, String inputPath, String outputPath) throws IOException {
        JobConf job = new JobConf(config, LongestLengthJob.class);
        job.setJarByClass(LongestLengthJob.class);
        job.setJobName("Longest Length Job");
        job.setInputFormat(KeyValueTextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //job.setNumMapTasks(16);
        //job.setNumReduceTasks(4);
        //job.setMaxMapAttempts(20);
        //job.setMaxReduceAttempts(20);
        //job.setMemoryForMapTask(300);
        //job.setMemoryForReduceTask(200);
        job.setMapperClass(LongestLengthMapper.class);
        job.setCombinerClass(LongestLengthReducer.class);
        job.setReducerClass(LongestLengthReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        return job;
    }

    public static class LongestLengthMapper implements Mapper<Text, Text, LongWritable, IntWritable> {
        @Override
        public void map(Text key, Text value, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
            output.collect(new LongWritable(Long.parseLong(key.toString())), new IntWritable(Integer.parseInt(value.toString())));
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void configure(JobConf job) {

        }
    }

    public static class LongestLengthReducer implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        int max = -1;

        @Override
        public void configure(JobConf job) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void close() throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void reduce(LongWritable key, Iterator<IntWritable> values, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                int current = values.next().get();
                if (current > max) {
                    max = current;
                    output.collect(key, new IntWritable(max));
                }
            }
        }
    }
}
