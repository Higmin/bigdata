package bd.mapreduce.log_analysis;

import bd.io.AdMetricWritable;
import bd.io.ReduceSideJoinWritable;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;

/**
 * @Auther : guojianmin
 * @Date : 2019/5/16 08:05
 * @Description : 需求5 实现reduce 的 join 操作
 *
 * 将表1和表2 关联起来，计算2019年1月7号 不同地域的曝光量，点击量，用户数
 * 表1：日期，地域编码，曝光量，点击量
 * 表2：日期，地域编码，用户数
 * 结果表：日期，地域编码，曝光量，点击量，用户数
 * 等价sql: select tb1.area, tb1.pv, tb1.click, tb2.user_count
 *             from tb1 join tb2 on  tb1.area_id = tb2.area_id
 *
 * 实现思路：
 * map阶段：根据文件名称区分文件（表）,然后通过每一行数据的分隔符，根据不同文件取到不同的，需要的数据
 *         然后将<area_id,Entity> 输出到reduce中（将相同地域编码的数据，由同一个reduce进行处理）
 * reduce阶段：area_id相同的 调用一次reduce处理 进行整合统计，将数据整理成结果表的一条数据，放在Entity中，然后输出<Null，Entity>
 *
 */
public class ReduceSideJoinMRJobNew extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //1.配置job
        Configuration conf = this.getConf();
        Job job = null;

        //2.创建job
        job = Job.getInstance(conf);
        job.setJarByClass(ReduceSideJoinMRJobNew.class);//设置通过主类来获取job

        //3.给job设置执行流程
        //3.1 HDFS中需要处理的文件路径(多文件路径)
        Path[] inputPath = new Path[args.length-1];
        for (int i = 0; i < inputPath.length-1; i++){
            inputPath[i] = new Path(args[i]);
        }
        FileInputFormat.setInputPaths(job,inputPath);

        //3.2设置map执行流程
        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);//设置map输出key的类型
        job.setMapOutputValueClass(ReduceSideJoinWritable.class);//设置map输出value的类型

        //3.2设置reduce执行流程
        job.setReducerClass(ReduceSideJoinReducer.class);
        job.setOutputKeyClass(NullWritable.class);//设置reduce输出key的类型
        job.setOutputValueClass(ReduceSideJoinWritable.class);//设置reduce输出value的类型

        //因为要确定reduce Task 的数量，以便于不同的终端类型分配到不同的reduce Task 所以要设置ReduceTasks
//        job.setNumReduceTasks(4);//硬编码，不友好，建议通过传参的方式实现

//        job.setPartitionerClass(TerminalTypePartitioner.class);//设置自定义的 Partitioner

        //3.4设置计算结果输出路径
        Path output = new Path(args[args.length-1]);
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
    public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, ReduceSideJoinWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();//获取文件名称
            String line = value.toString();
            String[] fields = line.split("\t");
            //由于前两列都是日期和地域ID 所以就直接写在外面了
            String date = fields[0];
            String areaId = fields[1];
            long userCount = 0;
            long pv = 0;
            long click = 0;
            String flag = "";
            if (fileName.startsWith("sum_user_count")){
                //处理的是按照日期和地域统计的用户数
                userCount = Long.valueOf(fields[2]);
                flag = "1";
            }else {
                pv = Long.valueOf(fields[2]);
                click = Long.valueOf(fields[3]);
                flag = "2";
            }
            ReduceSideJoinWritable outputValue = new ReduceSideJoinWritable(date,areaId,pv,click,userCount,flag);
            context.write(new Text(areaId),outputValue);
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
    public static class ReduceSideJoinReducer extends Reducer<Text, ReduceSideJoinWritable, NullWritable, ReduceSideJoinWritable> {

        @Override
        protected void reduce(Text key, Iterable<ReduceSideJoinWritable> values, Context context) throws IOException, InterruptedException {
            String date = "";
            String areaId = key.toString();
            long userCount = 0;
            long pv = 0;
            long click = 0;
            for (ReduceSideJoinWritable value : values){
                if (value.getFlag().equals("1")){
                    date = value.getDate();
                    userCount = value.getUserCount();
                }else if (value.getFlag().equals("2")){
                    pv = value.getPv();
                    click = value.getClick();
                }
            }
            ReduceSideJoinWritable outputValue = new ReduceSideJoinWritable(date,areaId,pv,click,userCount,null);
            context.write(NullWritable.get(),outputValue);
        }

    }

    public static void main(String[] args) {

        //用于本地测试
        if (args.length == 0){
            args = new String[]{
                "hdfs://ns/mr_project/report/sum_user_count_by_area_20190107.txt",
                "hdfs://ns/mr_project/output/SumGroupByAreaMRJob/sum_group_by_area",
                "hdfs://ns/mr_project/log_analysis/output5/ReduceSideJoin"
            };
        }
        Configuration conf = new Configuration();
        Path hdfsOutPutPath = new Path(args[args.length-1]);
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
            int status = ToolRunner.run(conf, new ReduceSideJoinMRJobNew(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //hadoop jar xxxx.jar mainclass -Dmapreduce.job.reduce=3    设置reduce的数量

    }
}
