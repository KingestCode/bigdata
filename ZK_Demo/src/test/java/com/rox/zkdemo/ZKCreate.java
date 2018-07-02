package com.rox.zkdemo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZKCreate {
    private static final String CONNECT_STRING = "cs1:2181,cs2:2181,cs3:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private static ZooKeeper zk = null;

    public static void main(String[] args) throws Exception {
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, null);
        boolean createZNode = ZKUtil.createZNode("/f/u/c/k", "bb", zk);
        if(createZNode) {
            System.out.println("创建成功！");
        }else {
            System.out.println("创建失败");
        }

        zk.close();
    }

}

class ZKUtil {
    public static boolean createZNode(String znodePath, String data, ZooKeeper zk) throws Exception {

        //看看要创建的节点是否存在
        if ((zk.exists(znodePath, false)) != null) {
            return false;
        } else {
            //获取父路径
            String parentPath = znodePath.substring(0, znodePath.lastIndexOf("/"));
            //如果父路径的长度大于0，则先创建父路径，再创建子路径
            if (parentPath.length() > 0) {
                createZNode(parentPath, data, zk);
                zk.create(znodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                //如果父路径的长度=0，则直接创建子路径
                zk.create(znodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            return true;
        }
    }
}
