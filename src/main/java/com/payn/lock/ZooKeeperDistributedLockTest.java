package com.payn.lock;

import java.util.concurrent.CountDownLatch;

/**
 * @author: payn
 * @date: 2020/12/15 10:07
 */
public class ZooKeeperDistributedLockTest {

	public static void main(String[] args) {
		// 使用CountDownLunch控制线程同时执行
		CountDownLatch countDownLatch = new CountDownLatch(1);
		// 开启3个线程模拟分布式环境，分布式环境下每个进程都是一个单独的zkClient
		Thread t1 = new Thread(new TestThread(countDownLatch));
		Thread t2 = new Thread(new TestThread(countDownLatch));
		Thread t3 = new Thread(new TestThread(countDownLatch));
		t1.start();
		t2.start();
		t3.start();

		System.out.println("休眠1秒后执行..." + System.currentTimeMillis());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// 倒计时结束
		countDownLatch.countDown();
	}
}

// 线程，尝试在zk上创建临时节点，创建成功则获得锁(执行权)
class TestZkLock implements Runnable {
	// 共享变量
	private static Integer CNT = 0;
	private CountDownLatch countDownLatch;

	public TestZkLock(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}

	public void run() {
		//初始化连接
		ZooKeeperDistributedLock zklock = new ZooKeeperDistributedLock("test001");
		String threadName = Thread.currentThread().getName();

		//竞争锁
		while (true) {
			try {
				System.out.println(threadName + " 开始竞争锁...");
				zklock.tryLock();
				// 获得锁后修改共享变量
				CNT++;
				System.out.println(threadName + " 释放了锁..." + CNT);
				zklock.unlock();
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// 创建临时节点失败，表示未获得锁
				System.out.println(threadName + " 未获得锁，将重试！！！");
				try {
					Thread.sleep(1500);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
}