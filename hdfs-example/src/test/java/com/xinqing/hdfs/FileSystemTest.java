package com.xinqing.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * FileSystemUsage测试
 *
 * Created by xuan on 2018/4/3
 */
public class FileSystemTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemTest.class);

    private FileSystem fs;
    private Configuration conf;

    @Before
    public void setup() throws IOException, InterruptedException {
        conf = new Configuration();
        fs = FileSystem.get(URI.create("hdfs://111.231.238.23:9000"), conf, "root");
    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("/"), true);
        while (it.hasNext()) {
            LocatedFileStatus fileStatus = it.next();
            LOG.info("fileStatus: {}", fileStatus);
        }
    }

    @Test
    public void mkdirs() throws IOException {
        fs.mkdirs(new Path("/test/a"));
    }

    @Test
    public void copyFromLocalFile() throws IOException {
        fs.copyFromLocalFile(new Path("/download/rabbitmq-server-windows-3.7.3.zip"), new Path("/test/a"));
    }

    @Test
    public void copyToLocalFile() throws IOException {
        fs.copyToLocalFile(new Path("/test/a/hadoop-2.6.0-cdh5.14.0.tar.gz"), new Path("/download"));
    }

    @Test
    public void deleteOnExit() throws IOException {
        fs.deleteOnExit(new Path("/test/a/rabbitmq-server-windows-3.7.3.zip"));
    }

    @After
    public void destroy() throws IOException {
        fs.close();
        conf = null;
    }
}
