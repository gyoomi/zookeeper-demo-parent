package com.gyoomi.hdfs.principle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * 类功能描述
 *
 * @author Leon
 * @version 2020/7/27 22:39
 */
public class Test01
{

	public FileSystem fs = null;

	@Before
	public void init() throws Exception
	{
		// 参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
		Configuration conf = new Configuration();
		// conf.set("fs.defaultFS", "hdfs://mini01:9000");
		// fs = FileSystem.get(conf);
		fs = FileSystem.get(new URI("hdfs://mini04:9000"), conf, "hadoop");
	}

	/**
	 * 查看目录信息，只显示文件
	 * @throws Exception e
	 */
	@Test
	public void testListAll() throws Exception
	{
		RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path("/"), true);
		while (ri.hasNext())
		{
			LocatedFileStatus cur = ri.next();
			System.out.println(cur.getPath().getName());
		}
	}

	/**
	 * 查看文件及文件夹信息
	 * @throws Exception e
	 */
	@Test
	public void testListStatus() throws Exception
	{
		FileStatus[] fss = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : fss)
		{
			if (fileStatus.isFile())
			{
				System.out.println("f-- " + fileStatus.getPath().getName());
			}
			if (fileStatus.isDirectory())
			{
				System.out.println("d-- " + fileStatus.getPath().getName());
			}
		}
	}

	/**
	 * hdfs上传文件
	 * @throws Exception e
	 */
	@Test
	public void testUpload() throws Exception
	{
		Path srcFile = new Path("D:\\哈哈哈的压缩包.zip");
		Path destination = new Path("/");
		fs.copyFromLocalFile(srcFile, destination);
		fs.close();
		System.out.println("ok");
	}

	/**
	 * 从hdfs中复制文件到本地文件系统
	 * @throws Exception e
	 */
	@Test
	public void testDownload() throws Exception
	{
		fs.copyToLocalFile(new Path("/哈哈哈的压缩包.zip"), new Path("d://"));
		fs.close();
		System.out.println("over");
	}

}
