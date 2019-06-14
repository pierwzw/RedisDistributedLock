package com.pier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

import java.util.Collections;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

/**
 * @auther zhongweiwu
 * @date 2019/6/13 17:17
 */
public class RedisDistributedLock {

    private static final Logger logger = LoggerFactory.getLogger(RedisDistributedLock.class);

    private static final String HOST = "aliyun";

    private static final int PORT = 6379;

    private static final int CONNECTION_TIMEOUT = 1000;

    private static final String SET_IF_NOT_EXIST = "nx";

    private static final String SET_WITH_EXPIRE_TIME = "px";

    /**
     * 锁前缀
     */
    private static final String ROOT_KEY = "LOCK_";

    /**
     * 过期时间，ms
     */
    private static final long EXPIRE = 15000L;

    /**
     * 最长等待时间，ms
     */
    private static final long WAIT_MILLIS = 10000L;

    /**
     * 重试等待时间，ms
     */
    private static final long SLEEP_MILLIS = 500L;

    /**
     * 最多重试次数
     */
    private static final int RETRIES = Integer.MAX_VALUE;

    /**
     * 使用 ThreadLocal 存储 key 的 value 值，防止同步问题
     */
    private static ThreadLocal<String> threadLocal = new ThreadLocal<>();

    /**
     * redis client
     */
    private static Jedis jedis;

    /**
     * 原子操作释放锁 Lua 脚本
     */
    private static final String LUA_UNLOCK_SCRIPT = "if redis.call(\"get\", KEYS[1]) == ARGV[1] " +
            "then " +
            "return redis.call(\"del\", KEYS[1]) " +
            "else " +
            "return 0 " +
            "end";

    static {
        jedis = getClient(HOST, PORT, CONNECTION_TIMEOUT);
    }

    private static Jedis getClient(String ip, int port, int timeout) {
        if (jedis != null) {
            return jedis;
        }
        JedisShardInfo jedisShardInfo = new JedisShardInfo(ip, port, timeout);
        jedisShardInfo.setPassword("644886");
        jedis = new Jedis(jedisShardInfo);
        return jedis;
    }

    public static boolean lock(String key) {
        return setLock(key, EXPIRE, WAIT_MILLIS, SLEEP_MILLIS, RETRIES);
    }
    
    public static boolean lock(String key, long waitMillis) {
        return setLock(key, EXPIRE, waitMillis, SLEEP_MILLIS, RETRIES);
    }

    public static boolean lock(String key, long waitMillis, long sleepMillis) {
        return setLock(key, EXPIRE, waitMillis, SLEEP_MILLIS, RETRIES);
    }

    public static boolean lock(String key, long expire, long waitMillis, long sleepMillis) {
        return setLock(key, expire, waitMillis, sleepMillis, RETRIES);
    }

    public static boolean lock(String key, long expire, long waitMillis, long sleepMillis, int retries) {
        return setLock(key, expire, waitMillis, sleepMillis, retries);
    }

    /**
     * 获取 Redis 锁
     *
     * @param key         锁名称
     * @param expire      锁过期时间
     * @param retries     最多重试次数
     * @param sleepMillis 重试等待时间
     * @param waitMillis  最长等待时间
     * @return
     */
    private static boolean setLock(String key, long expire, long waitMillis, long sleepMillis, int retries) {
        //检查 key 是否为空
        if (key == null || "".equals(key)) {
            return false;
        }

        try {
            // 用于超时重试
            long startTime = System.currentTimeMillis();
            key = ROOT_KEY + key;

            //可重入锁判断
            String v = threadLocal.get();
            if (v != null && isReentrantLock(key, v)) {
                return true;
            }

            //获取锁
            String value = UUID.randomUUID().toString();
            while (jedis.set(key, value, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expire) == null) {
                //超过最大重试次数后获取锁失败
                if (retries-- < 1) {
                    return false;
                }

                //等待下一次尝试
                Thread.sleep(sleepMillis);

                //超过最长等待时间后获取锁失败
                if (System.currentTimeMillis() - startTime > waitMillis) {
                    return false;
                }
            }

            threadLocal.set(value);
            return true;

        } catch (Exception e) {
            logger.error("redis lock get: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 可重入锁判断
     */
    private static boolean isReentrantLock(String key, String v) {
        String value = jedis.get(key);
        if (value == null) {
            return false;
        }

        return v.equals(value);
    }

    /**
     * 释放锁
     */
    public static void release(String key) {

        String token = threadLocal.get();
        if (StringUtils.isBlank(token)) {
            return;
        }
        try {
            jedis.eval(LUA_UNLOCK_SCRIPT, Collections.singletonList(key), Collections.singletonList(token));
        } catch (Exception e) {
            logger.error("redis lock release: {}", e.getMessage());
        }finally {
            threadLocal.remove();
        }
    }

    /**
     * 删除 redis key
     * <p>集群模式和单机模式执行脚本方法一样，但没有共同的接口
     * <p>使用lua脚本删除redis中匹配value的key，可以避免由于方法执行时间过长而 redis 锁自动过期失效的时候误删其他线程的锁
     */
    /*private boolean deleteKey(List<String> keys, List<String> args) {
        Object result = stringRedisTemplate.execute((RedisCallback<Object>) connection -> {
            Object nativeConnection = connection.getNativeConnection();

            // 单机模式
            if (nativeConnection instanceof Jedis) {
                return ((Jedis) nativeConnection).eval(LUA_UNLOCK_SCRIPT, keys, args);
            }

            // 集群模式
            else if (nativeConnection instanceof JedisCluster) {
                return ((JedisCluster) nativeConnection).eval(LUA_UNLOCK_SCRIPT, keys, args);
            }

            return 0L;
        });

        return result != null && Long.parseLong(result.toString()) > 0;
    }*/

    public static void main(String[] args) {
        for (int i = 0; i < 2; i++) {
            new Thread(new Runnable() {
                String key = "cheng";

                @Override
                public void run() {
                    try {
                        boolean lock = RedisDistributedLock.lock(key, 30);
                        System.out.print(lock + "-");
                    }catch (Exception e){
                        e.printStackTrace();
                    }finally {
                        RedisDistributedLock.release(key);
                    }
                }
            }).start();
        }
    }
}
