/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.ha;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式 - 客户端
 *
 * @author Leon
 * @date 2020-07-16 15:04
 */
public class DistributedClient
{

	private static final String CONNECT_STRING = "192.168.200.11:2181";

	private static final int SESSION_TIMEOUT = 20000;

	private static final String PARENT_NODE = "/servers";

	private volatile List<String> serverList;

	private ZooKeeper zk = null;

	public void getConnection() throws Exception
	{
		zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher()
		{
			@Override
			public void process(WatchedEvent event)
			{
				try
				{
					getServerList();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		});
	}

	public void getServerList() throws Exception
	{
		// 获取服务器子节点信息，并且对父节点进行监听
		List<String> children = zk.getChildren(PARENT_NODE, true);
		// 先创建一个局部的list来存服务器信息
		List<String> servers = new ArrayList<String>();
		for (String child : children)
		{
			// child只是子节点的节点名
			byte[] data = zk.getData(PARENT_NODE + "/" + child, false, null);
			servers.add(new String(data));
		}
		// 把servers赋值给成员变量serverList，已提供给各业务线程使用
		serverList = servers;

		//打印服务器列表
		System.out.println("服务器列表： " + serverList);
	}

	/**
	 * 业务功能
	 *
	 * @throws InterruptedException
	 */
	public void handleBiz(String hostname) throws InterruptedException
	{
		System.out.println(hostname + " client start working.....");
		Thread.sleep(Long.MAX_VALUE);
	}

	public static void main(String[] args) throws Exception
	{

		// 1. 获取zk连接
		DistributedClient client = new DistributedClient();
		client.getConnection();
		// 2. 获取servers的子节点信息（并监听），从中获取服务器信息列表
		client.getServerList();
		// 3. 业务线程启动
		client.handleBiz(args[0]);

	}
}


