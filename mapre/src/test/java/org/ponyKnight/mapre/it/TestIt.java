package org.ponyKnight.mapre.it;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.ponyKnight.mapre.MapreJobTrackerConnector;
import org.ponyKnight.mapre.sample.SampleMapreJob;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

public class TestIt {
	@Test
	public void testDummy() throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("---------------------------------------------------------");
		System.out.println("This is a integration test!");
		MapreJobTrackerConnector conn=new MapreJobTrackerConnector();
		Job job = SampleMapreJob.createJob(conn.getConfig(), "hdfs://10.9.109.80:9000/testIn.txt", "hdfs://10.9.109.80:9000/testOut.txt");//"hdfs://10.9.109.80:9000/testOut.txt"
		job.setWorkingDirectory(new Path("/tmp"));
		job.submit();
		boolean success = job.waitForCompletion(true);
		assertEquals(true, success);
		System.out.println("---------------------------------------------------------");
	}
}
