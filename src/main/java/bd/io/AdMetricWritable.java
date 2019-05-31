package bd.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AdMetricWritable implements Writable {

    private long pv;
    private long click;
    private float clickRate;

    //反序列化时，需要调用无参的构造方法，如果定义了有参的构造方法，一定要定义一个无参的构造方法
    public AdMetricWritable() {
    }

    public AdMetricWritable(long pv, long click, float clickRate) {
        this.pv = pv;
        this.click = click;
        this.clickRate = clickRate;
    }

    /**
     *用于序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.pv);
        dataOutput.writeLong(this.click);
        dataOutput.writeFloat(this.clickRate);
    }

    /**
     * 用于反序列化(反序列化的顺序要和序列化的顺序一致)
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pv = dataInput.readLong();
        this.click = dataInput.readLong();
        this.clickRate = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return this.pv + "\t" + this.click + "\t" +this.clickRate;
    }

    public long getPv() {
        return pv;
    }

    public void setPv(long pv) {
        this.pv = pv;
    }

    public long getClick() {
        return click;
    }

    public void setClick(long click) {
        this.click = click;
    }

    public float getClickRate() {
        return clickRate;
    }

    public void setClickRate(float clickRate) {
        this.clickRate = clickRate;
    }
}
