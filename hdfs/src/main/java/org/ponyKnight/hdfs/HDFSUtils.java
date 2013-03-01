package org.ponyKnight.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: rliu1
 * Date: 13-2-25
 * Time: 上午1:29
 * To change this template use File | Settings | File Templates.
 */
public class HDFSUtils {
    public static RemoteIterator<LocatedFileStatus> listFile(String directory, boolean recursive, HDFSConnector conn) throws IOException {
        FileSystem fs = conn.getFileSystem();
        return fs.listFiles(new Path(directory), recursive);
    }

    public static void putFile(File file, String target, boolean override, HDFSConnector conn) throws IOException {
        FileSystem fs = conn.getFileSystem();
        FSDataOutputStream out = fs.create(new Path(target), override);
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        byte[] buf = new byte[102400];
        int read = 0;
        while((read=in.read(buf))!=-1)
            out.write(buf,0,read);
        out.flush();
        out.close();
        in.close();
    }

    public static void rmFile(String path, boolean recursive, HDFSConnector conn) throws IOException {
        FileSystem fs = conn.getFileSystem();
        fs.delete(new Path(path), recursive);
    }

    public static FSDataInputStream openFile(String path, HDFSConnector conn) throws IOException {
        FileSystem fs = conn.getFileSystem();
        return fs.open(new Path(path));
    }

    public static void copyFile(String src, String desc, HDFSConnector conn) throws IOException {
        FileSystem fs = conn.getFileSystem();
        fs.copyFromLocalFile(new Path(src), new Path(desc));
    }

    public static void moveFile(String src, String desc, HDFSConnector conn) throws IOException {

    }
}
