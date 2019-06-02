package bd.mapreduce.log_analysis;

import bd.io.AdMetricWritable;
import bd.io.ComplexKeyWritable;
import bd.io.PVGroupComparator;
import bd.lib.PVPartitioner;
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
 * @Description : 需求2 自定义value
 *
 * 需求7 ：对需求6的计算结果中，各省市的曝光量，点击量数据进行排序
 * 排序规则：先按照曝光量降序排列，曝光量相同再按照点击量降序排列
 *
 * 实现思路：利用mapreduce的排序过程
 * 	1. 在map端的shuffle阶段（两次排序）
 * 		--->>inputFormat 从inputpath 中 读取文件到map中
 * 		1.1 map 端输出的时候，会在partitioner（为了确定输出到哪个reduce Task中,默认使用hashPartitioner，可使用job.setPartitionerClass(）自定义Partitioner）后掉用key的compareTo方法进行一次排序，
 * 		     然后局部聚合Combiner， 可以通过自定义key重写compareTo方法自定义排序规则。   单个的  临时文件1 ：<a, 1> <a, 1> <b,1> <b,1>  临时文件2 ：<a, 1> <a, 1> <b,1> <b,1>
 * 		1.2 ==>如果产生多个临时文件，这里需要将多个临时文件合并成一个大的文件，
 * 		     这一阶段叫MergeSort,这个阶段会对每个分区文件里面的数据进行一次排序，然后再进行一次局部聚合Combiner。单个的 临时文件合并完成之后：<a, 1> <a, 1> <a, 1> <a, 1> <b,1> <b,1> <b,1> <b,1>
 * 	2. reduce的shuffle阶段（两次排序）
 * 		2.1 收到map发送的数据之后，reduce会合并（MergeSort）Map Task 拷贝过来的数据，默认调用key的compareTo方法 进行一次排序。可以通过自定义key重写compareTo方法自定义排序规则
 * 		     合并的 一个大的 临时文件：  [ <a, 1> <a, 1> <a, 1> <a, 1> <b,1> <b,1> <b,1> <b,1> ]
 * 		2.2 合并之后分组，默认按照key分组，可以通过job.setGroupingComparatorClass();进行自定义分组规则  [ <a, 1> <a, 1> <a, 1> <a, 1> ]   [ <b,1> <b,1> <b,1> <b,1> ]
 * 	      	---->reduce统计  ，在每个reduce Task 中 ，相同key（这里判断key是否相等，是调用key的compareTo方法）的一组值调用一次reduce方法进行统计
 * 		（值得说明的是：我们当我们key中有多个字段，需要使用（复合组合键） 来排序的时候，在这里可以自定义分组器（job.setGroupingComparatorClass();），来减少reduce的调用次数。实现：自定义分组器（继承WritableComparator）  a.无参构造中调用父类的构造来注册:mapKey,和Boolean类型的是否创建字符比较集实例（true）  b.重写分组器的compare方法(判断mapKey中的分区键是否相等)，用来判断是否放在同一组,在来实现 分区key 相同的调用一次reduce方法）
 * 		-->将结果 outputFormat输出 到 outputPath 中<a,4> <b,4>
 */
public class SecondSortMRJobNew extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //1.配置job
        Configuration conf = this.getConf();
        Job job = null;

        //2.创建job
        job = Job.getInstance(conf);
        job.setJarByClass(SecondSortMRJobNew.class);//设置通过主类来获取job

        //3.给job设置执行流程
        //3.1 HDFS中需要处理的文件路径
        Path path = new Path(args[0]);
        //job添加输入路径
        FileInputFormat.addInputPath(job, path);

        //3.2设置map执行流程
        job.setMapperClass(SecondSortMapper.class);
        job.setMapOutputKeyClass(ComplexKeyWritable.class);//设置map输出key的类型
        job.setMapOutputValueClass(Text.class);//设置map输出value的类型

        job.setPartitionerClass(PVPartitioner.class);//自定义partitioner 使用map 输出key 的一部分（pv）进行分区处理

        //3.2设置reduce执行流程
        job.setReducerClass(SecondSortReducer.class);
        job.setOutputKeyClass(Text.class);//设置reduce输出key的类型
        job.setOutputValueClass(ComplexKeyWritable.class);//设置reduce输出value的类型

        //job.setNumReduceTasks(3);//硬编码，不友好，建议通过传参的方式实现

        job.setGroupingComparatorClass(PVGroupComparator.class);//自定义分组比较器。默认通过key的compareTo方法比较

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
    public static class SecondSortMapper extends Mapper<LongWritable, Text, ComplexKeyWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //处理的数据是需求6 的结果数据 （日期，地域名称，曝光量，点击量，用户数）：20190107	   广东	 25	 13	 24
            String line = value.toString();
            String[] fields = line.split("\t");
            long pv = Long.valueOf(fields[2]);
            long click = Long.valueOf(fields[3]);
            ComplexKeyWritable complexKey = new ComplexKeyWritable(pv,click);
            String outputValue = fields[0] + "\t" +fields[1] + "\t" + fields[4];
            //输出要注意：我们这里的key 是一个组合键（曝光量+点击量），默认的partitioner 是按照key的哈希值取余数，来确定放在哪个reduce Task里处理
            //我们要做的是将 曝光量 相同的 放在同一个reduce Task 而不是 曝光量和点击量都相同的 放在同一个reduce Task里处理
            //所以我们要自定义partitioner，实现思路是： 获取key 的一部分 作为分区键，然后根据 分区键 来确定reduce Task 编号
            context.write(complexKey,new Text(outputValue));

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
    public static class SecondSortReducer extends Reducer<ComplexKeyWritable, Text, Text, ComplexKeyWritable> {
        @Override
        protected void reduce(ComplexKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long pv = key.getPv();
            long click = key.getClick();
            for (Text value : values){
                String line = value.toString();
                String[] fields = line.split("\t");
                String date = fields[0];
                String areaName = fields[1];
                String userCount = fields[2];
                String outputValue = date + "\t" + areaName+ "\t" + pv + "\t" + click + "\t" +userCount;
                context.write(new Text(outputValue),key);
            }

        }
    }

    public static void main(String[] args) {

        //用于本地测试
        if (args.length == 0){
            args = new String[]{
                "hdfs://ns/mr_project/log_analysis/output6/MapSideJoin",
                "hdfs://ns/mr_project/log_analysis/output7"
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
            int status = ToolRunner.run(conf, new SecondSortMRJobNew(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //hadoop jar xxxx.jar mainclass -Dmapreduce.job.reduce=3    设置reduce的数量

    }
}
