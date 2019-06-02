package bd.lib;

import bd.io.AdMetricWritable;
import bd.io.ComplexKeyWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 自定义Partitioner 分区键
 *
 */
public class PVPartitioner extends Partitioner<ComplexKeyWritable,Text> {


    /**
     * 根据 map 输出key 来 获取分区键，并重新定义发送到reduce Task 的 算法（默认是对mapKey 去哈希值，然后对reduec Task 个数取余）
     * @param mapKey map 输出key
     * @param mapValue map 输出value
     * @param numReduceTasks Reduce Task 个数
     * @return reduce Task 编号
     */
    @Override
    public int getPartition(ComplexKeyWritable mapKey, Text mapValue, int numReduceTasks) {

        return (int)mapKey.getPv() % numReduceTasks;
    }
}
