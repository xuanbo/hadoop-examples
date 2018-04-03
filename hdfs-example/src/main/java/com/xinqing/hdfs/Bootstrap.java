package com.xinqing.hdfs;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * 启动
 *
 * Created by xuan on 2018/4/3
 */
public class Bootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) {
        FileSystemUsage fileSystemUsage = new FileSystemUsage(URI.create("hdfs://localhost:9000"));
        try {
            fileSystemUsage.listFiles(new Path("/"), true);
        } catch (IOException e) {
            LOG.error("listFiles exception", e);
        }
    }

}
