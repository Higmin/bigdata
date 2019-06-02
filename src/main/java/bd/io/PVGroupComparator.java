package bd.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PVGroupComparator extends WritableComparator {

    /**
     *  无参构造方法：调用父类的构造方法
     *  两个参数：mapKey,和Boolean类型的是否创建字符比较集实例（true）
     */
    public PVGroupComparator(){
        super(ComplexKeyWritable.class,true);
    }

    /**
     * 重写Compare方法，比较key是否相等
     * 相等的话就放在一组 返回 0
     * 不相等就放在不同的组中 返回 1
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComplexKeyWritable key1 = (ComplexKeyWritable) a;
        ComplexKeyWritable key2 = (ComplexKeyWritable) b;
        return key1.getPv() == key2.getPv() ? 0 :1;
    }
}
