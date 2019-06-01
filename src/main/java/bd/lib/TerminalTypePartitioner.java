package bd.lib;

import bd.io.AdMetricWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 自定义Partitioner 用于根据 终端ID 发送到不同的Reduce Task
 * 自定义Partitioner 会在map 输出之后 调用 自定义Partitioner ，
 * 然后根据 map 输出key 的类型 来获取到 终端id
 * 然后根据终端id 获取到 Reduce Task （这里省略了一步 ：根据终端id获取到终端类型，然后根据终端类型获取到对应的Reduce Task）
 * 发送到不同的Reduce Task 进行处理
 *
 */
public class TerminalTypePartitioner extends Partitioner<Text,AdMetricWritable> {


    //模拟不同的终端id 对应不用的 Reduce Task
    public static HashMap<String,Integer> terminalTaskMap = new HashMap<>();
    static {
        /**
         * 1.1 安卓手机
         * 1.2 苹果手机
         * 2.1 安卓平板
         * 2.2 ipad
         * 3.1 电脑
         * 4.1 H5
         * 5.1 小程序
         * 6.1 微信公众号
         *
         *
         * 移动端 的数据 发送到Reduce task0
         * PC端 的数据 发送到Reduce task1
         * WeChat 移动端 的数据 发送到Reduce task2
         * 其他位置设备 的数据 发送到Reduce task3
         */
        terminalTaskMap.put("1.1",0);
        terminalTaskMap.put("1.2",0);
        terminalTaskMap.put("2.1",0);
        terminalTaskMap.put("2.2",0);
        terminalTaskMap.put("3.1",1);
        terminalTaskMap.put("4.1",0);
        terminalTaskMap.put("5.1",2);
        terminalTaskMap.put("6.1",2);
    }

    @Override
    public int getPartition(Text key, AdMetricWritable value, int numReduceTasks) {
        String terminalId = key.toString();
        return terminalTaskMap.get(terminalId) == null ? 3 : terminalTaskMap.get(terminalId);
    }
}
