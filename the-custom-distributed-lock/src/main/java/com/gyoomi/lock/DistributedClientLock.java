/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.lock;

import org.apache.zookeeper.*;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Distributed Lock Demo
 *
 * @author Leon
 * @date 2020-07-17 8:59
 */
public class DistributedClientLock
{

	private static final int SESSION_TIMEOUT = 20000;

	private static final String ZK_HOST = "192.168.200.11:2181";

	private static final String PARENT_NODE = "locks";

	private static final String NODE = "lock";

	private boolean haveLock = false;

	private ZooKeeper zk;

	/**
	 * 记录自己创建的子节点路径
	 */
	private volatile String thisPath;

	public void connectZookeeper() throws Exception
	{
		zk = new ZooKeeper(ZK_HOST, SESSION_TIMEOUT, new Watcher()
		{
			@Override
			public void process(WatchedEvent event)
			{
				try
				{
					List<String> children = zk.getChildren("/" + PARENT_NODE, true);
					String thisNode = thisPath.substring(("/" + PARENT_NODE + "/").length());
					Collections.sort(children);
					// 去比较是否自己是最小id
					if (children.indexOf(thisNode) == 0)
					{
						doSomething();
						// 重新注册一把新的锁
						thisPath = zk.create("/" + PARENT_NODE + "/" + NODE, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		});

		// 1. 程序一启动就先注册一把锁到zk
		thisPath = zk.create("/" + PARENT_NODE + "/" + NODE, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		Thread.sleep(new Random().nextInt(1000));
		// 2. 如果争抢资源的程序就只有自己，则可以直接去访问共享资源
		doSomething();
		thisPath = zk.create("/" + PARENT_NODE + "/" + NODE, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

	}

	/**
	 * 处理业务逻辑，并且在最后释放锁
	 */
	private void doSomething() throws Exception
	{
		try
		{
			System.out.println("gain lock: " + thisPath);
			Thread.sleep(2000);
			// do something
		}
		finally
		{
			System.out.println("finished: " + thisPath);
			zk.delete(this.thisPath, -1);
		}
	}

	public static void main(String[] args) throws Exception
	{
		DistributedClientLock dl = new DistributedClientLock();
		dl.connectZookeeper();
		Thread.sleep(Long.MAX_VALUE);
	}
}
