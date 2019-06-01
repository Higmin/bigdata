package bd.mapreduce.log_analysis;

import bd.io.AdMetricWritable;
import bd.io.PVKeyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Auther : guojianmin
 * @Date : 2019/5/16 08:05
 * @Description : 需求3 自定义键，实现对需求2 结果进行 逆序 排列
 *
 *  日志文件名 ：date(例如：20190107)
 *  等价table 字段： id , advertiser_id , duration , position , area_id , terminal_id , view_type , device_id , date
 *  等价sql : select date,sum(view_type=1) pv , sum(view_type=2)click, click/pv as clickRate
 *           from log_table where date >= ‘20190101’ and date <='20190107'
 *           order by pv desc
 *
 *  实现思路：map 阶段：获取到需求2的数据，然后发送到reduce
 *          reduce 阶段：shuffle阶段 排序 默认是key的升序。可以通过自定义key（重写compareTo方法） 输出
 *
 */
public class OrderByDescMRJobNew extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //1.配置job
        Configuration conf = this.getConf();
        Job job = null;

        //2.创建job
        job = Job.getInstance(conf);
        job.setJarByClass(OrderByDescMRJobNew.class);//设置通过主类来获取job

        //3.给job设置执行流程
        //3.1 HDFS中需要处理的文件路径
        Path path = new Path(args[0]);
        //job添加输入路径
        FileInputFormat.addInputPath(job, path);

        //3.2设置map执行流程
        job.setMapperClass(OrderByDescMapper.class);
        job.setMapOutputKeyClass(PVKeyWritable.class);//设置map输出key的类型
        job.setMapOutputValueClass(Text.class);//设置map输出value的类型

        //3.2设置reduce执行流程
        job.setReducerClass(OrderByDescReducer.class);
        job.setOutputKeyClass(Text.class);//设置reduce输出key的类型
        job.setOutputValueClass(AdMetricWritable.class);//设置reduce输出value的类型

        //job.setNumReduceTasks(3);//硬编码，不友好，建议通过传参的方式实现

        //3.4设置计算结果输出路径
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);

        //4.提交job，并等待job执行完成
        boolean result = job.waitForCompletion(true);//等待job执行完成
        return result ? 0 : 1;
    }

    //map阶段

    /***
     * 输入数据键值对类型
     * LongWritable : 输入数据的偏移量
     * Text：输入数据类型
     *
     * 输出数据键值对类型
     * Text：输出数据key的类型
     * AdMetricWritable：输出数据value类型
     */
    public static class OrderByDescMapper extends Mapper<LongWritable, Text, PVKeyWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //20190101	1025	475	0.46341464
            String line = value.toString();
            String[] fields = line.split("\t");
            String date = fields[0];
            Long pv = Long.valueOf(fields[1]);
            Long click = Long.valueOf(fields[2]);
            float clickRate = Float.valueOf(fields[3]);
            PVKeyWritable pvKey = new PVKeyWritable();//自定义的key类型，可以按照pv降序排列
            pvKey.setPv(pv);

            String val = date + "\t" + click + "\t" + clickRate;//自定义输出格式，中间用换行符 \t 分割
            context.write(pvKey,new Text(val));

        }
    }

    //Reduce阶段

    /**
     * 输入数据键值对类型
     * Text:
     * AdMetricWritable:
     * <p>
     * 输出数据键值对类型
     * Text:
     * ntWritable:
     */
    public static class OrderByDescReducer extends Reducer<PVKeyWritable, Text, Text, AdMetricWritable> {
        @Override
        protected void reduce(PVKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value:values) {
                String[] valStrs = value.toString().split("\t");
                AdMetricWritable ad = new AdMetricWritable();
                ad.setPv(key.getPv());
                ad.setClick(Long.valueOf(valStrs[1]));
                ad.setClickRate(Float.valueOf(valStrs[2]));
                context.write(new Text(valStrs[0]),ad);//输出的时候  会调用AdMetricWritable的tostring 方法
            }
        }
    }

    public static void main(String[] args) {

        //用于本地测试
        if (args.length == 0){
            args = new String[]{
                "hdfs://ns/mr_project/log_analysis/output2",
                "hdfs://ns/mr_project/log_analysis/output3"
            };
        }
        Configuration conf = new Configuration();
        Path hdfsOutPutPath = new Path(args[1]);
        try {
            //如果输出路径存在，则删除
            FileSystem fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(hdfsOutPutPath)){
                fileSystem.delete(hdfsOutPutPath,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            int status = ToolRunner.run(conf, new OrderByDescMRJobNew(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //hadoop jar xxxx.jar mainclass -Dmapreduce.job.reduce=3    设置reduce的数量

    }
}
