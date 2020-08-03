package com.gyoomi.hdfs.principle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
		FSDataOutputStream fsDataOutputStream = fs.create(new Path("/admin.log"), true);
		FileInputStream fis = new FileInputStream("d://admin.log");
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

	/**
	 * 按照最近的block进行读取
	 */
	@Test
	public void testSpecialCat() throws Exception
	{
		FSDataInputStream fsDataInputStream = fs.open(new Path("/admin.log"));
		// 1. 拿到这个文件的信息
		FileStatus[] fileStatuses = fs.listStatus(new Path("/admin.log"));
		// 2. 获取当前文件的所有block元数据
		BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatuses[0], 0L, fileStatuses[0].getLen());
		// 3.1  第一个block的长度
		long length = fileBlockLocations[0].getLength();
		// 3.2 第一个block的起始偏移量
		long offset = fileBlockLocations[0].getOffset();

		// System.out.println("length: " + length);
		// System.out.println("offset: " + offset);
		// IOUtils.copyBytes(fsDataInputStream, System.out, (int)length);

		byte[] buffer = new byte[4096];
		FileOutputStream fos = new FileOutputStream(new File("d://1.log"));
		while (fsDataInputStream.read(offset, buffer, 0, 4096) != -1)
		{
			fos.write(buffer);
			offset += 4096;
			if (offset >= length)
			{
				return;
			}
		}

		fos.flush();
		fos.close();
		fsDataInputStream.close();

		System.out.println("over");
	}

}
