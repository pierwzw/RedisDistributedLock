package com.pier.redisson;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.config.Config;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @auther zhongweiwu
 * @date 2019/6/15 10:34
 */
public class DistributedLock {

    private static CountDownLatch finish = new CountDownLatch(2);
    private static final String KEY = "testlock";
    private static Config config;
    private static Redisson redisson;
    static {
        config = new Config();
        config.useSingleServer().setAddress("redis://aliyun:6379").setPassword("644886");
        redisson = (Redisson)Redisson.create(config);
    }

    public static void main(String[] args) {
        Thread thread_1 = new Thread(new Worker("thread-1"));
        Thread thread_2 = new Thread(new Worker("thread-2"));
        thread_1.start();
        try {
            TimeUnit.SECONDS.sleep(10); // 睡10秒钟 为了让thread_1充分运行
            thread_2.start();
            TimeUnit.SECONDS.sleep(10); // 让thread_2 等待锁
            thread_2.interrupt(); // 中断正在等待锁的thread_2 观察thread_2是否会不会拿到锁
            finish.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            redisson.shutdown();
        }
    }

    static class Worker implements Runnable {
        private String name;

        public Worker(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            RLock lock = redisson.getLock(KEY);
            lock.lock(10, TimeUnit.MINUTES);
            if (!Thread.currentThread().isInterrupted()) {
                System.out.println(Thread.currentThread().getName() + " gets lock.");
                try {
                    TimeUnit.MINUTES.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                }
            } else {
                System.out.println(Thread.currentThread().getName() + " does not get lock.");
            }
            System.out.println(Thread.currentThread().getName() + " ends.");
        }
    }
}
