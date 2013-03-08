package org.ponyKnight.mapre.sample;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class SampleMapreJob {
	public static Job createJob(Configuration config, String inputPath, String outputPath) throws IOException {
		Job job = Job.getInstance(config);
		job.setJarByClass(SampleMapreJob.class);
		job.setJobName("Sample Mapre Job");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setMapperClass(LineLengthMapper.class);
        job.setReducerClass(LineLengthReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
		return job;
	}

	public static class LineLengthMapper extends
			Mapper<LongWritable, Text, LongWritable, IntWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("Running...");
			context.write(key, new IntWritable(value.toString().length()));
		}
	}

	public static class LineLengthReducer extends
			Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		public void reduce(LongWritable key, Iterator<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int length = 0;
			if (values.hasNext())
				length = values.next().get();
			context.write(key, new IntWritable(length));
		}
	}
}
