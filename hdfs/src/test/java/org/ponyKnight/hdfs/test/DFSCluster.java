package org.ponyKnight.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;


public class DFSCluster {

    private static MiniDFSCluster cluster = null;
    private static Configuration conf = null;

    private static final int BYTES_PER_CHECKSUM = 4;
    private static final int PACKET_SIZE = BYTES_PER_CHECKSUM;
    private static final int BLOCK_SIZE = 2 * PACKET_SIZE;
    private static final int DATANODE_NUM = 5;

    public static MiniDFSCluster ensureCluster() throws IOException{
        if (cluster == null)
            createCluster();
        return cluster;
    }

    private static void init(Configuration conf) {
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);
    }

    public static void createCluster() throws IOException {
        conf = new HdfsConfiguration();
        init(conf);
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM).
                nameNodePort(8020).build();
    }

}
