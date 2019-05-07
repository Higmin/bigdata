package bd.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Auther : guojianmin
 * @Date :
 * @Description : TODO用一句话描述此类的作用
 */
public class ZookeeperClient {
    private static String connectString = "192.168.183.150:2181,192.168.183.151:2181,192.168.183.152:2181";
    private static int sessionTimeout = 5000;
    private static ZooKeeper zk = null;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static Stat stat = new Stat();

    public static void getConn() throws IOException {
        //String connectString, int sessionTimeout, Watcher watche
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //作用：客户端接收服务端发送的监听通知事件
                System.out.println("收到监听通知！");
                System.out.println("type:"+event.getType() + "  path:"+event.getPath());
                if (event.getState() == Event.KeeperState.SyncConnected){
                    //服务端像客户端发送建立连接成功的通知
                    if (event.getType() == Event.EventType.None && event.getPath() == null){
                        System.out.println("zookeeper客户端与服务端成功建立连接！");
                        countDownLatch.countDown();
                    }else if (event.getType() == Event.EventType.NodeChildrenChanged){
                        System.out.println("通知："+event.getPath()+"节点的子节点发生变化");
                        try {
                            ZookeeperClient.getChildrenNodes(event.getPath(),true);//重新注册监听事件
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }else if (event.getType() == Event.EventType.NodeDataChanged){
                        System.out.println("通知："+event.getPath()+"节点数据发生变化");
                        try {
                            ZookeeperClient.getZnodeData(event.getPath(),true);//重新注册监听事件
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
    }

    //创建节点
    public static void createZnode(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {
        if (zk != null){
            //String path, byte[] data, List<ACL> acl, CreateMode createMode
            //znode路径 ，znode值，znode访问权限,znode类型
            zk.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,createMode);
        }
    }
    //获取节点下的所有子节点
    public static void getChildrenNodes(String path, boolean watch) throws KeeperException, InterruptedException {
        if (zk != null){
            //String path, boolean watch
            List<String> children = zk.getChildren(path, watch);
            System.out.println(path+"所有字节点："+children);
        }
    }

    //获取节点数据
    public static void getZnodeData(String path, boolean watch) throws KeeperException, InterruptedException {
        if (zk != null){
            //String path, boolean watch, Stat stat
            //节点路径，是否添加监听事件，节点状态信息对象
            byte[] data = zk.getData(path, watch, stat);
            System.out.println("获取到的"+path+"的数据为："+new String(data));
            System.out.println("czxid:"+stat.getCzxid()+" mzxid:"+stat.getMzxid());
        }
    }

    //更新节点数据
    public static Stat setZnodeData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        Stat stat1 = null;
        if (zk != null){
            //String path, byte[] data, int version
            //节点路径，待更新数据，更新的版本号
            // version=-1表示znode基于最新的版本号进行更新，version从0开始更新操作传入的版本号与最新的版本号一致，否则更新失败
            stat1 = zk.setData(path, data, version);
            System.out.println(path+"节点信息:czxid="+stat1.getCzxid()+"mzxid="+stat1.getMzxid()+"版本号:"+stat1.getVersion());
        }
        return stat1;
    }

    //删除节点
    public static void deleteZnode(String path, int version) throws KeeperException, InterruptedException {
        if (zk != null){
            //String path, int version
            //节点路径.要删除节点的版本号
            //只允许删除叶子节点，不能删除带有子节点的嵌套节点
            zk.delete(path,version);
            System.out.println("删除"+path+"节点");
        }
    }

    public static void main(String[] args) {
//        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境。
        try {
            ZookeeperClient.getConn();
            System.out.println("当前连接状态："+zk.getState());
            countDownLatch.await();//等待服务端向客户端发送建立连接的通知
            //1.创建znode节点
            //注意：创建重名的节点会抛出异常org.apache.zookeeper.KeeperException$NodeExistsException: KeeperErrorCode = NodeExists for /zk-test1
//            String path = "/zk-test1";
//            ZookeeperClient.createZnode(path,"hello zhtest1".getBytes(),CreateMode.PERSISTENT);
            //注意：不能创建包含子节点的节点会抛出异常org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for /zk-test2/temp1
//            String path = "/zk-test2/temp1";
//            ZookeeperClient.createZnode(path,"hello zhtest1".getBytes(),CreateMode.PERSISTENT);
            //2.获取节点下的所有子节点
            ZookeeperClient.getChildrenNodes("/zk-test1",true);
//            ZookeeperClient.createZnode("/zk-test1/temp1","temp1".getBytes(),CreateMode.PERSISTENT);
            //3.获取节点数据
            ZookeeperClient.getZnodeData("/zk-test1",true);
            //4.更新节点数据
//            Stat stat1 = ZookeeperClient.setZnodeData("/zk-test1", "111".getBytes(), -1);
//            Stat stat2 = ZookeeperClient.setZnodeData("/zk-test1", "222".getBytes(), stat1.getVersion());
            //注意：更新操作传入的版本号与最新的版本号不一致，导致更新失败  org.apache.zookeeper.KeeperException$BadVersionException: KeeperErrorCode = BadVersion for /zk-test1
//            Stat stat3 = ZookeeperClient.setZnodeData("/zk-test1", "333".getBytes(), stat1.getVersion());
            //5.删除节点
            //注意：如果删除的不是叶子节点 会抛出异常 org.apache.zookeeper.KeeperException$NotEmptyException: KeeperErrorCode = Directory not empty for /zk-test1
//            ZookeeperClient.deleteZnode("/zk-test1",5);
            //注意：如果删除的版本号与实际版本号不一致 会抛出异常org.apache.zookeeper.KeeperException$BadVersionException: KeeperErrorCode = BadVersion for /zk-test1/temp1
//            ZookeeperClient.deleteZnode("/zk-test1/temp1",1);
//            ZookeeperClient.deleteZnode("/zk-test1/temp1",0); //删除成功
            ZookeeperClient.deleteZnode("/zk-test1",5);//删除子节点之后，删除成功

            Thread.sleep(Integer.MAX_VALUE);
            System.out.println("客户端运行结束");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
