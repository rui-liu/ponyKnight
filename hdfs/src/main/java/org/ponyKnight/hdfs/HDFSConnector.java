package org.ponyKnight.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: rliu1
 * Date: 13-2-25
 * Time: 上午1:29
 * To change this template use File | Settings | File Templates.
 */
public class HDFSConnector {

    Configuration conf;
    public HDFSConnector() {
        this("10.9.109.80","9000");
    }

    public HDFSConnector(String host, String port){
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://"+host+":"+port+"/");
        System.setProperty("HADOOP_USER_NAME","hadoop");
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(conf);
    }
}
