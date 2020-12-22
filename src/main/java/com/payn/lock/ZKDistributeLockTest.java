package com.payn.lock;

import org.I0Itec.zkclient.ZkClient;

import java.util.concurrent.CountDownLatch;

/**
 * ZK 实现分布式锁测试
 *
 * @author: payn
 * @date: 2020/12/15 9:44
 */
public class ZKDistributeLockTest {

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
class TestThread implements Runnable {
	// 共享变量
	private static Integer CNT = 0;
	private ZkClient zkClient;
	private CountDownLatch countDownLatch;

	public TestThread(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}

	// 连接zk
	private void connect() {
		String threadName = Thread.currentThread().getName();
		try {
			System.out.println(threadName + " 等待执行...");
			// 等待倒计时结束
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(threadName + " 请求连接zk..." + System.currentTimeMillis());
		zkClient = new ZkClient("192.168.25.128:2181", 20000);
		System.out.println(threadName + " 连接成功...");
		// 输出目录信息测试
//        List<String> children = zkClient.getChildren("/");
//        children.forEach(System.out::println);
	}

	public void run() {
		// 初始化连接(在各个线程里开启连接，模拟分布式环境)
		connect();
		String threadName = Thread.currentThread().getName();

		// 竞争锁
		while (true) {
			try {
				System.out.println(threadName + " 开始竞争锁...");
				// 创建zk临时节点
				zkClient.createEphemeral("/dl", "test");
				System.out.println(threadName + " 获得锁！！！");
				// 获得锁后修改共享变量
				CNT++;
				System.out.println(threadName + " 释放了锁..." + CNT);
				zkClient.delete("/dl");
				Thread.sleep(2000);
			} catch (Exception e) {
				// 创建临时节点失败，表示未获得锁
				System.out.println(threadName + " 未获得锁，将重试！！！");
//                System.out.println(e.getMessage());
				try {
					Thread.sleep(1500);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
}


/*
休眠1秒后执行...1607997904581
Thread-0 等待执行...
Thread-1 等待执行...
Thread-2 等待执行...
Thread-1 请求连接zk...1607997905582
Thread-2 请求连接zk...1607997905582
Thread-0 请求连接zk...1607997905582
Thread-1 连接成功...
Thread-1 开始竞争锁...
Thread-0 连接成功...
Thread-0 开始竞争锁...
Thread-2 连接成功...
Thread-2 开始竞争锁...
Thread-0 获得锁！！！
Thread-0 释放了锁...1
Thread-2 未获得锁，将重试！！！
Thread-1 未获得锁，将重试！！！
Thread-2 开始竞争锁...
Thread-1 开始竞争锁...
Thread-1 获得锁！！！
Thread-1 释放了锁...2
Thread-2 未获得锁，将重试！！！
Thread-0 开始竞争锁...
Thread-0 获得锁！！！
Thread-0 释放了锁...3
* */