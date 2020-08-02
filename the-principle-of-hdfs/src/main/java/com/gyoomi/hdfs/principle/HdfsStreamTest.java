package com.gyoomi.hdfs.principle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * hdfs 流式操作
 *
 * @author Leon
 * @version 2020/8/1 23:54
 */
public class HdfsStreamTest
{

	private FileSystem fs = null;


	@Before
	public void init() throws Exception
	{
		Configuration configuration = new Configuration();
		fs = FileSystem.get(new URI("hdfs://mini04:9000"), configuration, "hadoop");
	}

	/**
	 * 通过流的方式上传文件到hdfs
	 */
	@Test
	public void testUpload() throws Exception
	{
		FSDataOutputStream fsDataOutputStream = fs.create(new Path("/zyy.txt"), true);
		FileInputStream fis = new FileInputStream("d://zyy.txt");
		IOUtils.copyBytes(fis, fsDataOutputStream, 1024 * 8);
	}

	/**
	 * 下载
	 */
	@Test
	public void testDownloadFileToLocal() throws Exception
	{
		FSDataInputStream fsDataInputStream = fs.open(new Path("/zyy.txt"));
		FileOutputStream fos = new FileOutputStream(new File("d://1.txt"));
		IOUtils.copyBytes(fsDataInputStream, fos, 4096);
	}

	/**
	 * hdfs支持随机定位进行文件读取，而且可以方便地读取指定长度。
	 * 用于上层分布式运算框架并发处理数据。
	 */
	@Test
	public void testRandomAccess() throws Exception
	{
		// 1. 先获取一个文件的输入流----针对hdfs上的
		FSDataInputStream inputStream = fs.open(new Path("/zyy.txt"));
		// 2. 可以将流的起始偏移量进行自定义
		inputStream.seek(22);

		FileOutputStream fos = new FileOutputStream(new File("d://1.txt"));
		IOUtils.copyBytes(inputStream, fos, 4096);
	}

	/**
	 * 显示hdfs上文件的内容
	 */
	@Test
	public void testCat() throws Exception
	{
		FSDataInputStream dataInputStream = fs.open(new Path("/zyy.txt"));

		IOUtils.copyBytes(dataInputStream, System.out, 1024);
	}

}
