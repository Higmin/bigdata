package bd.mapreduce.log_analysis;

import bd.io.AdMetricWritable;
import bd.io.TerminalTypeReportWritable;
import bd.lib.TerminalTypePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;

/**
 * @Auther : guojianmin
 * @Date : 2019/5/16 08:05
 * @Description : 需求4 自定义partitioner
 *
 * 统计2019年 1月7日，各终端类型的曝光量，点击量，一个结果文件存储一种终端类型文件
 *
 * 实现思路：根据 终端ID 发送到不同的Reduce Task ,自定义partitioner
 * map阶段：拆分每条日志数据，将需要的terminal_id，和view_type 找出来放在Entity中 ，然后以<terminal_id,Entity>发送给reduce
 * 自定义partitioner：1.重写getPartition 方法
 *                   2.然后根据 map 输出key 的类型 来获取到 终端id，
 *                   2.然后根据终端id 获取到要发送的 Reduce Task 编号，并发送到对应的Reduce Task上（即返回值为对应的编号）
 * reduce阶段：1.setup reduce开始的时候调用一次(这里用于初始化终端ID和终端类型的对应关系)
 *            2.reduce 通过终端id 获取到终端类型，然后将终端类型，和曝光量，点击量的统计放在Entity中。
 *            3.cleanup 在reduce 结束的时候调用一次 ，通常用于释放连接等，这里用于输出统计结果
 */
public class CaseWhenSumGroupByMRJobNew extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //1.配置job
        Configuration conf = this.getConf();
        Job job = null;

        //2.创建job
        job = Job.getInstance(conf);
        job.setJarByClass(CaseWhenSumGroupByMRJobNew.class);//设置通过主类来获取job

        //3.给job设置执行流程
        //3.1 HDFS中需要处理的文件路径
        Path path = new Path(args[0]);
        //job添加输入路径
        FileInputFormat.addInputPath(job, path);

        //3.2设置map执行流程
        job.setMapperClass(CaseWhenSumGroupByMapper.class);
        job.setMapOutputKeyClass(Text.class);//设置map输出key的类型
        job.setMapOutputValueClass(AdMetricWritable.class);//设置map输出value的类型

        //3.2设置reduce执行流程
        job.setReducerClass(CaseWhenSumGroupByReducer.class);
        job.setOutputKeyClass(NullWritable.class);//设置reduce输出key的类型
        job.setOutputValueClass(TerminalTypeReportWritable.class);//设置reduce输出value的类型

        //因为要确定reduce Task 的数量，以便于不同的终端类型分配到不同的reduce Task 所以要设置ReduceTasks
        job.setNumReduceTasks(4);//硬编码，不友好，建议通过传参的方式实现

        job.setPartitionerClass(TerminalTypePartitioner.class);//设置自定义的 Partitioner

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
    public static class CaseWhenSumGroupByMapper extends Mapper<LongWritable, Text, Text, AdMetricWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //id , advertiser_id , duration , position , area_id , terminal_id , view_type , device_id , date
            String line = value.toString();
            String[] fields = line.split("\t");

            String terminal_id = fields[5];//终端id
            String viewType = fields[6];
            if (viewType != null && !viewType.equals("")){
                AdMetricWritable adMetric = new AdMetricWritable();
                int viewTypeInt = Integer.parseInt(viewType);
                if (viewTypeInt == 1){//曝光
                    adMetric.setPv(1);
                }else if (viewTypeInt == 2){
                    adMetric.setClick(1);
                }
                context.write(new Text(terminal_id),adMetric);
            }

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
     * AdMetricWritable:
     */
    public static class CaseWhenSumGroupByReducer extends Reducer<Text, AdMetricWritable, NullWritable, TerminalTypeReportWritable> {

        //注意：这里有个问题：因为发送到同一 reduce Task 的 数据，可能是不同的终端类型（比如移动端：安卓手机，苹果手机等）
        //所以要注意 统计不同终端类型需要做区分处理 （这里用terminalTypeReport 做区分处理）
        private TerminalTypeReportWritable terminalTypeReport = new TerminalTypeReportWritable();
        private HashMap<String,String> terminalTypeMap = new HashMap<>();
        /**
         * setup 初始化方法 reduce开始的时候调用一次(这里用于初始化终端ID和终端类型的对应关系)
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
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
            terminalTypeMap.put("1.1","Mobile");
            terminalTypeMap.put("1.2","Mobile");
            terminalTypeMap.put("2.1","Mobile");
            terminalTypeMap.put("2.2","Mobile");
            terminalTypeMap.put("3.1","PC");
            terminalTypeMap.put("4.1","Mobile");
            terminalTypeMap.put("5.1","WeChat");
            terminalTypeMap.put("6.1","WeChat");
        }

        @Override
        protected void reduce(Text key, Iterable<AdMetricWritable> values, Context context) throws IOException, InterruptedException {
            if ("".equals(terminalTypeReport.getTerminalType())){
                String terminalType = terminalTypeMap.get(key.toString());//通过终端id获取到终端类型
                terminalTypeReport.setTerminalType(terminalType);
            }
            for (AdMetricWritable value : values){
                terminalTypeReport.plusPv(value.getPv());
                terminalTypeReport.plusClick(value.getClick());
            }
        }

        /**
         * cleanup 在reduce 结束的时候调用一次 ，通常用于释放连接等
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),terminalTypeReport);
        }
    }

    public static void main(String[] args) {

        //用于本地测试
        if (args.length == 0){
            args = new String[]{
                "hdfs://ns/mr_project/ad_log/ad_20190107.txt",
                "hdfs://ns/mr_project/log_analysis/output4"
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
            int status = ToolRunner.run(conf, new CaseWhenSumGroupByMRJobNew(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //hadoop jar xxxx.jar mainclass -Dmapreduce.job.reduce=3    设置reduce的数量

    }
}
