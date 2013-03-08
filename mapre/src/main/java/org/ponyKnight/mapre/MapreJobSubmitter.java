package org.ponyKnight.mapre;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class MapreJobSubmitter {
	public static boolean submitJob(Job job) throws ClassNotFoundException, IOException, InterruptedException{
		job.submit();
		return true;
	}
}
