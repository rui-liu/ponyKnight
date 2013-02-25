package org.ponyKnight.web.pages;

import org.apache.click.Page;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.ponyKnight.hdfs.HDFSConnector;

import java.io.IOException;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 13-2-23
 * Time: 下午6:30
 * To change this template use File | Settings | File Templates.
 */
public class HelloWorld extends Page {
    private Date time = new Date();

    public HelloWorld() {
        HDFSConnector connector = new HDFSConnector();
        try {
            FileSystem fs = connector.getFileSystem();
            RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), false);
            String paths = "";
            while(iter.hasNext()){
                paths += iter.next().getPath().getName() + "<br/>";
            }
            addModel("time", paths);

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
