package com.xinqing.hdfs;

import org.apache.hadoop.fs.Path;
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
public class FileSystemUsageTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemUsageTest.class);

    private FileSystemUsage fileSystemUsage;

    @Before
    public void setup() {
        fileSystemUsage = new FileSystemUsage(URI.create("hdfs://111.231.238.23:9000"));
    }

    @Test
    public void list() {
        try {
            fileSystemUsage.listFiles(new Path("/"), true);
        } catch (IOException e) {
            LOG.error("listFiles exception", e);
        }
    }

}
