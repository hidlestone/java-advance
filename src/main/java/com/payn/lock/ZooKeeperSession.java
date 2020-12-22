package com.payn.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * zk 分布式锁，其实可以做的比较简单，就是某个节点尝试创建临时 znode，此时创建成功了就获取了这个锁；
 * 这个时候别的客户端来创建锁会失败，只能注册个监听器监听这个锁。
 * 释放锁就是删除这个 znode，一旦释放掉就会通知客户端，然后有一个等待着的客户端就可以再次重新加锁。
 *
 * @author: payn
 * @date: 2020/12/14 17:22
 */
public class ZooKeeperSession {

	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

	private ZooKeeper zookeeper;
	private CountDownLatch latch;

	public ZooKeeperSession() {
		try {
			this.zookeeper = new ZooKeeper("192.168.25.128:2181", 50000, new ZooKeeperWatcher());
			try {
				connectedSemaphore.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("ZooKeeper session established......");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取分布式锁
	 *
	 * @param productId
	 */
	public Boolean acquireDistributedLock(Long productId) {
		String path = "/product-lock-" + productId;
		long waitTime = 1000;
		try {
			zookeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			return true;
		} catch (Exception e) {
			while (true) {
				try {
					// 相当于是给node注册一个监听器，去看看这个监听器是否存在
					Stat stat = zookeeper.exists(path, true);

					if (stat != null) {
						this.latch = new CountDownLatch(1);
						this.latch.await(waitTime, TimeUnit.MILLISECONDS);
						this.latch = null;
					}
					zookeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
					return true;
				} catch (Exception ee) {
					continue;
				}
			}

		}
	}

	/**
	 * 释放掉一个分布式锁
	 *
	 * @param productId
	 */
	public void releaseDistributedLock(Long productId) {
		String path = "/product-lock-" + productId;
		try {
			zookeeper.delete(path, -1);
			System.out.println("release the lock for product[id=" + productId + "]......");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 建立 zk session 的 watcher
	 */
	public class ZooKeeperWatcher implements Watcher {

		private CountDownLatch latch = new CountDownLatch(1);

		public void process(WatchedEvent event) {
			System.out.println("Receive watched event: " + event.getState());

			if (Event.KeeperState.SyncConnected == event.getState()) {
				connectedSemaphore.countDown();
			}

			if (this.latch != null) {
				this.latch.countDown();
			}
		}

	}

	/**
	 * 封装单例的静态内部类
	 */
	private static class Singleton {

		private static ZooKeeperSession instance;

		static {
			instance = new ZooKeeperSession();
		}

		public static ZooKeeperSession getInstance() {
			return instance;
		}

	}

	/**
	 * 获取单例
	 *
	 * @return
	 */
	public static ZooKeeperSession getInstance() {
		return Singleton.getInstance();
	}

	/**
	 * 初始化单例的便捷方法
	 */
	public static void init() {
		getInstance();
	}

}
