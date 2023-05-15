package com.lzy.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisConnectionFactory {
    private static JedisPool jedisPool;

    static {
        JedisPoolConfig poolConfig=new JedisPoolConfig();
        poolConfig.setMaxTotal(8);
        poolConfig.setMinIdle(0);
        poolConfig.setMaxIdle(8);
        poolConfig.setMaxWaitMillis(1000);
        jedisPool=new JedisPool(poolConfig,"47.120.1.221",6379,1000,"qq1998928");
    }

    public static Jedis getJedis(){
        return jedisPool.getResource();
    }
}
