/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.ha;

import org.apache.zookeeper.*;

/**
 * 分布式 - 服务端
 *
 * @author Leon
 * @date 2020-07-16 14:50
 */
public class DistributedServer
{

	private static final String CONNECT_STRING = "192.168.200.11:2181";

	private static final int SESSION_TIMEOUT = 20000;

	private static final String PARENT_NODE = "/servers";

	private ZooKeeper zk = null;

	public void getConnection() throws Exception
	{
		zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher()
		{
			@Override
			public void process(WatchedEvent event)
			{
				// 收到事件通知后的回调函数（应该是我们自己的事件处理逻辑）
				System.out.println(event.getType() + "---" + event.getPath());

				try
				{
					zk.getChildren("/", true);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		});
	}

	public void registerServer(String hostname) throws Exception
	{
		String result = zk.create(PARENT_NODE + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostname + " is online ......" + result);
	}

	/**
	 * 业务功能
	 *
	 * @throws InterruptedException
	 */
	public void handleBiz(String hostname) throws InterruptedException
	{
		System.out.println(hostname + "start working.....");
		Thread.sleep(Long.MAX_VALUE);
	}

	public static void main(String[] args) throws Exception
	{
		DistributedServer ds = new DistributedServer();
		// 1. 获取zk连接
		ds.getConnection();
		// 2. 利用zk连接注册服务器信息
		ds.registerServer(args[0]);
		// 3. 启动业务功能
		ds.handleBiz(args[0]);
	}

}
