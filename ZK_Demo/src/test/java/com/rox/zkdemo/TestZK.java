package com.rox.zkdemo;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


/**
 * 对应关系:
 *  Linux   Java
 *  ls      getChildren
 *  get     getData
 *  set     setDAta
 *  create  create
 */

public class TestZK {
    @Test
    public void ls() throws IOException, KeeperException, InterruptedException {

        /**
         * 放三个就不行, 只能放2个目前来看
         */
        ZooKeeper zk = new ZooKeeper("cs1:2181,cs2:2181,cs3:2181", 5000, null);
        List<String> list = zk.getChildren("/", null);

        for (String s : list) {
            System.out.println(s);
        }
    }

    @Test
    public void lsAll() {
        try {
            ls("/");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 列出指定 path 下的children
     *
     * @param path
     */
    public void ls(String path) throws Exception {
        System.out.println(path);

        ZooKeeper zk = new ZooKeeper("cs1:2181,cs2:2181,cs3:2181", 5000, null);
        List<String> list = zk.getChildren(path, null);
        if (list == null || list.isEmpty()) {
            return;
        }
        for (String s : list) {
            // 先输出 children
            if (path.equals("/" )) {
                ls(path + s);
            } else {
                ls(path + "/" + s);
            }
        }
    }


    /**
     * 设置数据
     */
    @Test
    public  void  setData() throws  Exception {
        ZooKeeper zk = new ZooKeeper("cs1:2181", 5000, null);
        zk.setData("/a","tomaslee".getBytes(),0);
    }


    /**
     * 创建临时节点
     */
    @Test
    public void  createEPHEMERAL() throws  Exception {
        ZooKeeper zk = new ZooKeeper("cs1:2181", 5000, null);
        zk.create("/c/c1", "tom".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        System.out.println("hello");
    }



    /**
     * 创建观察者
     */
    @Test
    public void  testWatch() throws  Exception {
        final ZooKeeper zk = new ZooKeeper("cs1:2181,cs2:2181,cs3:2181", 5000, null);

        Stat st = new Stat();

        Watcher w = null;
        w = new Watcher() {
            public void process(WatchedEvent event) {

                try {
                    System.out.println("数据改了...");
                    zk.getData("/a",this,null);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        byte[] data = zk.getData("/a",w,st);
        System.out.println(new String(data));

        while (true) {
            Thread.sleep(1000);
        }
    }
}


