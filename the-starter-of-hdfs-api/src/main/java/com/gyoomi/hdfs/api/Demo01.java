package com.gyoomi.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * Hdfs的简单使用
 *
 * @author Leon
 * @version 2020/7/25 22:19
 */
public class Demo01
{

	public static void main(String[] args) throws Exception
	{
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://192.168.200.10:9000");
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.200.10:9000"), configuration, "hadoop");
		// 1. 上传文件
		// fs.copyFromLocalFile(new Path("d:/access.txt"), new Path("/access.txt.copy"));
		// 2. 下载文件
		fs.copyToLocalFile(new Path("/access.txt.copy"), new Path("d:/access.txt.copy"));
		fs.close();
		System.out.println("over");
	}

}
