package bd.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PVKeyWritable implements WritableComparable<PVKeyWritable> {

    private long pv;

    public PVKeyWritable() {
    }

    @Override
    public int compareTo(PVKeyWritable o) {
        //升序：当a > b ,返回一个正数，一般为1
        //降序：当a < b ,返回一个负数，一般为-1
        return this.pv > o.pv ? -1 :1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.pv);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pv = dataInput.readLong();
    }

    @Override
    public String toString() {
        return String.valueOf(this.pv);
    }

    public long getPv() {
        return pv;
    }

    public void setPv(long pv) {
        this.pv = pv;
    }
}
