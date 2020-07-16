/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

import org.apache.zookeeper.ZooKeeper;

/**
 * zk Test
 *
 * @author Leon
 * @date 2020-07-16 11:30
 */
public class ZkTest01
{

	public static final String CONNECT_STRING = "192.168.200.11:2181,192.168.200.12:2181,192.168.200.13:2181";

	public static final int TIMEOUT = 20000;

	public static void main(String[] args) throws Exception
	{
		ZooKeeper zk = new ZooKeeper(CONNECT_STRING, TIMEOUT, watchedEvent -> System.out.println("Watching: " + watchedEvent.getType() + "  " + watchedEvent.getPath()));

		// 1. 创建：数据节点到zk中
//		String result = zk.create("/idea", "idea zk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//		System.out.println(result);

		// 2. 判断： 判断znode节点是否存在
//		Stat exists = zk.exists("/idea", false);
//		System.out.println(exists);

		// 3. 获取： 获取子节点
//		List<String> children = zk.getChildren("/", true);
//		System.out.println(children);
//		Thread.sleep(Long.MAX_VALUE);

		// 4. 获取： 获取节点的数据
//		byte[] data = zk.getData("/idea", true, null);
//		System.out.println(new String(data));

		// 5. 删除： 删除znode节点
		// 参数2：指定要删除的版本，-1表示删除所有版本
//		zk.delete("/idea", -1);

		// 6.
	}
}
