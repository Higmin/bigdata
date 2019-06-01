package bd.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Auther : guojianmin
 * @Date : 2019/5/15 13:35
 * @Description : mapreduce统计单词 数量
 */
public class WordCountMRJob {


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
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");
            for (String word : words){
                //word 1
                context.write(new Text(word),new IntWritable(1));
                System.out.println("map key-->"+key+" map conut-->"+1);
            }

        }
    }

    //Reduce阶段

    /**
     * 输入数据键值对类型
     * Text:
     * IntWritable:
     *
     * 输出数据键值对类型
     * Text:
     * ntWritable:
     */
    public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //word {1,1,1.........}
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key,new IntWritable(sum));
            System.out.println("reduce key-->"+key+" reduce conut-->"+sum);
        }
    }

    public static void main(String[] args) {
        //1.配置job
        Configuration conf = new Configuration();
        Job job = null ;

        //2.创建job
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(WordCountMRJob.class);//设置通过主类来获取job

        //3.给job设置执行流程
        //3.1 HDFS中需要处理的文件路径
        Path path = new Path(args[0]);
        //job添加输入路径
        try {
            FileInputFormat.addInputPath(job,path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //3.2设置map执行流程
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);//设置map输出key的类型
        job.setMapOutputValueClass(IntWritable.class);//设置map输出value的类型

        //3.2设置reduce执行流程
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);//设置reduce输出key的类型
        job.setOutputValueClass(IntWritable.class);//设置reduce输出value的类型

        //3.4设置计算结果输出路径
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,output);

        //4.提交job，并等待job执行完成
        try {
            boolean result = job.waitForCompletion(true);//等待job执行完成
            System.exit(result ? 0 : 1 );
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
