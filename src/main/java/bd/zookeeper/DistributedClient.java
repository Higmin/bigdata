package bd.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @Auther : guojianmin
 * @Date :
 * @Description : TODO用一句话描述此类的作用
 */
public class DistributedClient {

    private static String connectString = "192.168.183.150:2181,192.168.183.151:2181,192.168.183.152:2181";
    private static int sessionTimeout = 5000;//客户端与服务端建立连接的超时时间
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static ZooKeeper zk = null;
    private static final String rootNode = "/servers";
    private static List<String> runningServerList = null ;//正在运行的服务器列表
    private static String connectedServer;//客户端已连接的服务器地址
    private static int clientNumber;

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
                    }else if (event.getType() == Event.EventType.NodeChildrenChanged){
                        System.out.println("重新获取服务器列表，并注册监听事件！");
                        try {
                            getRunningServers(event.getPath());//重新获取服务器列表，并注册监听事件
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

    /**
     * 获取正在运行的服务器列表
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void getRunningServers(String path) throws KeeperException, InterruptedException {
        runningServerList = zk.getChildren(path, true);
        System.out.println("获取到的所有服务器列表:"+runningServerList);
    }

    /**
     *获取客户端要连接的服务器地址
     * @param clientNumber
     * @return
     */
    public String getTagetServerAddress(int clientNumber){
        if (runningServerList != null && runningServerList.size() > 0){
            System.out.println("clientNumber:"+clientNumber);
            System.out.println("正在运行的服务器的个数为："+runningServerList.size());
            //算法：targetServer = clientNumber % serverList.size
            int serverIndex = clientNumber % runningServerList.size() ; //目标服务器在正在运行的服务器列表中的位置
            return runningServerList.get(serverIndex);
        }else {
            return null;
        }
    }

    /**
     * 获取客户端编号
     * @return
     */
    public int getClientNumber(){
        Random random = new Random();
        return random.nextInt(1000);
    }

    /**
     * 检测客户端已连接的服务器地址是否有效
     * @return
     */
    public boolean ifConnected(){
        if (runningServerList != null && runningServerList.size() > 0 && connectedServer != null){
            for (String server:runningServerList) {
                if (connectedServer.equals(server)){
                    System.out.println("连接有效");
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 执行具体业务功能，每隔2秒钟检测已连接的服务器地址是否有效，无效则重新去获取一个服务器地址
     * @param serverUrl
     */
    public void work(String serverUrl){
        System.out.println("serverUrl"+serverUrl);
        System.out.println("client working ...");
        connectedServer =serverUrl;
        while (true){
            System.out.println("已连接的服务器地址："+connectedServer);
            try {
                Thread.sleep(2000);
                if (!ifConnected()){
                    System.out.println(connectedServer+"连接失效，需要重新连接服务器地址!");
                    connectedServer = getTagetServerAddress(clientNumber);
                    System.out.println("重新连接"+connectedServer+"成功！");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        //1.获取zk连接
        DistributedClient client = new DistributedClient();
        try {
            client.getConn();
            countDownLatch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //2.从zk获取正在运行的服务器列表
        try {
            client.getRunningServers(rootNode);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //3.获取客户端要访问的服务器地址
        clientNumber = client.getClientNumber();
        String tagetUrl = client.getTagetServerAddress(clientNumber);
        //4.执行具体的业务功能
        client.work(tagetUrl);
    }
}
