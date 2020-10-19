package com.itheima.test;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.Arrays;

public class CeateDataTest {

    /**
     * 创建a节点
     * @throws Exception
     */
    @Test
    public void App() throws Exception {

       // 创建客户端

       //  connectString:连接zookeeper的服务单url
       String connectString = "localhost:2181";

       //  sessionTimeoutMs会话超时
       int sessionTimeoutMs = 3000;

       // 连接超时
       int connectionTimeoutMs = 3000;

       // retryPolicy: 重试策略
       RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3,3000);
       CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);

       client.start();

       // 创建一个空节点
//       client.create().forPath("/c");

      client.create().forPath("/a");
      client.create().forPath("/c");

       client.close();
   }

    /**
     * 删除a节点
     * @throws Exception
     */
  @Test
   public void DeleteDataTest () throws Exception {

       // 创建客户端
      // Zookeeper的url加端口
       String connectString = "localhost:2181";
       // sessionTimeoutMs：会话超时
       int sessionTimeoutMs = 3000;
       // connectionTimeoutMs:连接超时
       int connectionTimeoutMs = 3000;

       // 一个重试策略
       RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3,1000);

      CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);

      // 启动客户端
      client.start();

      // 删除a
      client.delete().guaranteed().forPath("/a");

      // 关闭连接
      client.close();
  }

    /**
     * 设置a节点为good
     * @throws Exception
     */
  @Test
  public void EditDataTest() throws Exception {
       String connectString = "localhost:2181";
       int sessionTimeoutMs = 3000;
       int connectionTimeoutMs = 3000;
       RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3,1000);
      CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
      client.start();
      client.setData().forPath("/c","good".getBytes());
      client.close();
  }

  @Test
  public void getDataTest() throws Exception {
        String connectString = "localhost:2181";
        int sessionTimeoutMs = 3000;
        int connectionTimeoutMs = 3000;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3,1000);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        client.start();
        byte[] bytes = client.getData().forPath("/c");
        System.out.println(Arrays.toString(bytes));
        client.close();
    }

    /**
     * 节点阻塞
     * @throws Exception
     */
    @Test
    public void NodeCacheTest() throws Exception {
        String connectString = "localhost:2181";
        int sessionTimeoutMs = 3000;
        int connectionTimeoutMs = 3000;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3,1000);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        client.start();
        NodeCache nodeCache = new NodeCache(client,"/c");
       // 启动缓存节点的数据
        nodeCache.start(true);
        System.out.println("当前节点的数据:"+new String(nodeCache.getCurrentData().getData()));
        // 添加监听
        nodeCache.getListenable().addListener(()->{
            System.out.println("节点数据变化了");
            System.out.println("更新后的节点数据:"+new String(nodeCache.getCurrentData().getData()));
        });
        // 进入阻塞状态 不要停止运行
        System.in.read();
    }

   @Test
    public void PathChildrenCacheTest() throws Exception {
        String connectString = "localhost:2181";
        int sessionTimeoutMs = 3000;
        int connectionTimeoutMs = 3000;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3,1000);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        client.start();
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client,"/c",true);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        System.out.println(pathChildrenCache.getCurrentData());
        pathChildrenCache.getListenable().addListener((cli,event)->{
            if(event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED){
                System.out.println("子节点更新");
                System.out.println("节点:"+event.getData().getPath());
                System.out.println("数据" + new String(event.getData().getData()));
            }else if(event.getType() == PathChildrenCacheEvent.Type.INITIALIZED ){
                System.out.println("初始化操作");
            }else if(event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED ){
                System.out.println("删除子节点");
                System.out.println("节点:"+event.getData().getPath());
                System.out.println("数据" + new String(event.getData().getData()));
            }else if(event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED ){
                System.out.println("添加子节点");
                System.out.println("节点:"+event.getData().getPath());
                System.out.println("数据" + new String(event.getData().getData()));
            }else if(event.getType() == PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED ){
                System.out.println("连接失效");
            }else if(event.getType() == PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED ){
                System.out.println("重新连接");
            }else if(event.getType() == PathChildrenCacheEvent.Type.CONNECTION_LOST ){
                System.out.println("连接失效后稍等一会儿执行");
            }
        });

        // 进入阻塞状态
        System.in.read();
    }

    @Test
    public void TreeCacheTest() throws Exception {
        String connectString = "localhost:2181";
        int sessionTimeoutMs = 3000;
        int connectionTimeoutMs = 3000;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3,1000);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        client.start();
        TreeCache treeCache = new TreeCache(client, "/a");
        treeCache.start();
        treeCache.getListenable().addListener((cli,event)->{
            if(event.getType() == TreeCacheEvent.Type.NODE_ADDED){
                System.out.println(event.getData().getPath() + "节点添加:" + new String(event.getData().getData()));
            }else if (event.getType() == TreeCacheEvent.Type.NODE_REMOVED){
                System.out.println(event.getData().getPath() + "节点移除");
            }else if(event.getType() == TreeCacheEvent.Type.NODE_UPDATED){
                System.out.println(event.getData().getPath() + "节点修改:" + new String(event.getData().getData()));
            }else if(event.getType() == TreeCacheEvent.Type.INITIALIZED){
                System.out.println("初始化完成");
            }else if(event.getType() == TreeCacheEvent.Type.CONNECTION_SUSPENDED){
                System.out.println("连接过时");
            }else if(event.getType() ==TreeCacheEvent.Type.CONNECTION_RECONNECTED){
                System.out.println("重新连接");
            }else if(event.getType() ==TreeCacheEvent.Type.CONNECTION_LOST){
                System.out.println("连接过时一段时间");
            }
        });
        System.in.read();
    }
}

