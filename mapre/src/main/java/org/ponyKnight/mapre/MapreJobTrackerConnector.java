package org.ponyKnight.mapre;

import org.apache.hadoop.conf.Configuration;

public class MapreJobTrackerConnector {
	private Configuration config;

	public MapreJobTrackerConnector() {
		this("10.9.109.80", "9001");
	}

	public MapreJobTrackerConnector(String host, String port) {
        System.setProperty("HADOOP_USER_NAME","hadoop");
		this.config = new Configuration();
		this.config.set("mapred.job.tracker", host + ":" + port);
		this.config.set("yarn.resourcemanager.address", host + ":" + port);
		this.config.set("mapreduce.framework.name", "yarn");
		this.config.set("fs.defaultFS", "hdfs://"+"10.9.109.80"+":"+"9000"+"/");
		this.config.set("yarn.resourcemanager.scheduler.address","10.9.109.80:9002");
		this.config.set("yarn.resourcemanager.resource-tracker.address","10.9.109.80:9003");
		
		//this.config.set("fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.Hdfs");
	}

	public Configuration getConfig() {
		return this.config;
	}
}
