package com.lzy.zookeeperdemo.utils;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZookeeperConn {
    private static final int BASE_SLEEP_TIME = 1000;
    private static final int MAX_RETRIES = 3;



    public static void main(String[] args) {
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(BASE_SLEEP_TIME,MAX_RETRIES);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                // the server to connect to (can be a server list)
                .connectString("8.130.11.118:2181")
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
    }



}
