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
        conf = new Configuration();
        conf.addResource(new Path("c:/core-site.xml"));
        conf.addResource(new Path("c:/hdfs-site.xml"));
        conf.set("fs.default.name", "hdfs://10.9.109.80:9000");
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(conf);
    }
}
