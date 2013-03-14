package org.ponyKnight.mapre.it;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
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
		JobConf job = SampleMapreJob.createJob(conn.getConfig(), "hdfs://10.9.109.80:9000/testIn.txt", "hdfs://10.9.109.80:9000/testOut.txt");//"hdfs://10.9.109.80:9000/testOut.txt"
		RunningJob runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();
        boolean success = runningJob.isSuccessful();
		assertEquals(true, success);
		System.out.println("---------------------------------------------------------");
	}
    public static void main(String[] args){
        TestIt ti = new TestIt();
        try {
            ti.testDummy();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
