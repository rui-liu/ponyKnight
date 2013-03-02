package org.ponyKnight.hdfs.test;

import org.ponyKnight.hdfs.HDFSConnector;
import org.ponyKnight.hdfs.HDFSUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.testng.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: rliu1
 * Date: 13-3-1
 * Time: 下午8:53
 * To change this template use File | Settings | File Templates.
 */

public class HDFSUtilsTest {
    HDFSConnector conn = null;
    File txtFile = null;
    File binaryFile = null;


    @BeforeClass
    public void init() throws Exception {
        DFSCluster.ensureCluster();
        conn = new HDFSConnector();
        txtFile = new File(this.getClass().getResource("/TextTestFile.txt").toURI());
    }

    @Test(groups = {"UnitTest", "HDFS"}, priority = 1)
    public void putTextFileOverrideTest() throws Exception {
        HDFSUtils.putFile(txtFile, "/" + txtFile.getName(), true, conn);
        assertTrue(true);
    }

    @Test(groups = {"UnitTest", "HDFS"}, priority = 2)
    public void putTextFileNonOverrideNegativeTest() {
        try {
            HDFSUtils.putFile(txtFile, "/" + txtFile.getName(), false, conn);
        } catch (Exception err) {
            assertTrue(IOException.class.isAssignableFrom(err.getClass()));
            return;
        }
        fail("A IOException should be throw!");
    }

    @Test(groups = {"UnitTest", "HDFS"}, priority = 3)
    public void openTextFileTest() throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(HDFSUtils.openFile("/" + txtFile.getName(), conn)));
        verifyTextContent(in);
    }

    @Test(groups = {"UnitTest", "HDFS"}, priority = 7)
    public void removeFileTest() throws Exception {
        HDFSUtils.deleteFile("/" + txtFile.getName(), false, conn);
    }

    private void verifyTextContent(BufferedReader in) throws IOException {
        String content = in.readLine();
        assertEquals(content, "This is a test file.");
        assertNull(in.readLine());
    }


}
