/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

import org.apache.zookeeper.*;

/**
 * zk Test
 *
 * @author Leon
 * @date 2020-07-16 11:30
 */
public class ZkTest01
{

	public static final String CONNECT_STRING = "192.168.200.11:2181";

	public static final int TIMEOUT = 10000;

	public static void main(String[] args) throws Exception
	{
		ZooKeeper zk = new ZooKeeper(CONNECT_STRING, TIMEOUT, watchedEvent -> System.out.println("Watching: " + watchedEvent.getType() + "  " + watchedEvent.getPath()));

		String result = zk.create("/idea", "idea zk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(result);
	}
}
