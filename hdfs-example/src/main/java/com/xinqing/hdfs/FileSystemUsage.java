package com.xinqing.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Hadoop FileSystem使用
 *
 * Created by xuan on 2018/4/3
 */
public class FileSystemUsage {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemUsage.class);

    /**
     * URI.create("hdfs://111.231.238.23:9000")
     */
    private final URI uri;

    public FileSystemUsage(URI uri) {
        this.uri = uri;
    }

    /**
     * 递归打印出Hadoop某个路径下的文件
     *
     * @param path Hadoop某个路径
     * @param recursive 是否递归
     * @throws IOException IOException
     */
    public void listFiles(final Path path, final boolean recursive) throws IOException {
        // 创建Configuration对象
        Configuration conf = new Configuration();
        // 创建FileSystem对象
        FileSystem fs = FileSystem.get(uri, conf);
        // 迭代根目录
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, recursive);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            LOG.info("fileStatus: {}", fileStatus);
        }
    }

}
