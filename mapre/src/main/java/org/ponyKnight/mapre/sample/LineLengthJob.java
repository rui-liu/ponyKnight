package org.ponyKnight.mapre.sample;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class LineLengthJob {
	public static JobConf createJob(Configuration config, String inputPath, String outputPath) throws IOException {
		JobConf job = new JobConf(config,LineLengthJob.class);
		job.setJarByClass(LineLengthJob.class);
		job.setJobName("Line Length Job");
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //job.setNumMapTasks(16);
        //job.setNumReduceTasks(1);
        //job.setMaxMapAttempts(20);
        //job.setMaxReduceAttempts(20);
        //job.setMemoryForMapTask(300);
        //job.setMemoryForReduceTask(200);
        job.setMapperClass(LineLengthMapper.class);
        job.setReducerClass(LineLengthReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setCompressMapOutput(true);
        //job.setMapOutputCompressorClass(LzoCodec.class);
		return job;
	}

	public static class LineLengthMapper implements Mapper<LongWritable, Text, LongWritable, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
            output.collect(key, new IntWritable(value.toString().length()));
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void configure(JobConf job) {

        }
    }

	public static class LineLengthReducer implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
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
            int length = 0;
            if (values.hasNext())
                length = values.next().get();
            output.collect(key, new IntWritable(length));
        }
    }
}
