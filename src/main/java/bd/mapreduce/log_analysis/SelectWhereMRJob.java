package bd.mapreduce.log_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * @Auther : guojianmin
 * @Date : 2019/5/16 08:05
 * @Description : 需求1 获取指定广告主的明细数据
 */
public class SelectWhereMRJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //1.配置job
        Configuration conf = this.getConf();
        Job job = null;

        //2.创建job
        job = Job.getInstance(conf);
        job.setJarByClass(SelectWhereMRJob.class);//设置通过主类来获取job

        //3.给job设置执行流程
        //3.1 HDFS中需要处理的文件路径
        Path path = new Path(args[0]);
        //job添加输入路径
        FileInputFormat.addInputPath(job, path);

        //3.2设置map执行流程
        job.setMapperClass(SelectWhereMapper.class);
        job.setMapOutputKeyClass(Text.class);//设置map输出key的类型
        job.setMapOutputValueClass(NullWritable.class);//设置map输出value的类型
        job.setNumReduceTasks(0);//MR默认是1 Reduce task
        //3.3设置reduce执行流程


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
     * IntWritable：输出数据value类型
     */
    public static class SelectWhereMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            if (fields != null && fields.length == 9){
                if (fields[1].equals("apple")){
                    context.write(new Text(line),NullWritable.get());
                }
            }
        }
    }

    //Reduce阶段

    /**
     * 输入数据键值对类型
     * Text:
     * IntWritable:
     * <p>
     * 输出数据键值对类型
     * Text:
     * ntWritable:
     */
    public static class SelectWhereReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //word {1,1,1.........}
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
            System.out.println("reduce key-->" + key + " reduce conut-->" + sum);
        }
    }

    public static void main(String[] args) {

        //用于本地测试
        if (args.length == 0){
            args = new String[]{
                    "hdfs://ns/mr_project/ad_log/ad_20190107.txt",
                    "hdfs://ns/mr_project/log_analysis/output1"
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
            int status = ToolRunner.run(conf, new SelectWhereMRJob(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //hadoop jar xxxx.jar mainclass -Dmapreduce.job.reduce=3    设置reduce的数量

    }
}
