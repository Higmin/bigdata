package bd.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @Auther : guojianmin
 * @Date :
 * @Description : TODO用一句话描述此类的作用
 */
public class DistributedServer {

    private static String connectString = "192.168.183.150:2181,192.168.183.151:2181,192.168.183.152:2181";
    private static int sessionTimeout = 5000;//客户端与服务端建立连接的超时时间
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static ZooKeeper zk = null;
    private static final String rootNode = "/servers";

    public void getConn() throws IOException {
        //String connectString, int sessionTimeout, Watcher watche
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //作用：客户端接收服务端发送的监听通知事件
                System.out.println("收到监听通知！");
                System.out.println("type:" + event.getType() + "  path:" + event.getPath());
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    //服务端像客户端发送建立连接成功的通知
                    if (event.getType() == Event.EventType.None && event.getPath() == null) {
                        System.out.println("zookeeper客户端与服务端成功建立连接！");
                        countDownLatch.countDown();
                    }
                }
            }
        });
    }

    /**
     *服务端程序向zk注册服务器信息
     */
    public void registerServer(String serverNodeName) throws KeeperException, InterruptedException {
        String serverNodePath = rootNode+"/"+serverNodeName;
        String rs = zk.create(serverNodePath, serverNodeName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        if (rs != null){
            System.out.println(serverNodeName+"节点以上线");
        }
    }

    /**
     * 执行具体的业务功能
     */
    public void work(){
        System.out.println("Server working ...");
        try {
            Thread.sleep(Long.MAX_VALUE);//模拟服务端程序作为后台服务长期运行
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        //1.获取zk连接
        DistributedServer server = new DistributedServer();
        server.getConn();
        countDownLatch.await();
        String hostname = args.length == 0 ? "localhost" : args[0];
        //2.注册Server相关信息
        server.registerServer(hostname);
        //3.执行具体的业务功能
        server.work();
    }
}
