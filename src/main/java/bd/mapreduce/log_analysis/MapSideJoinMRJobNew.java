package bd.mapreduce.log_analysis;

import bd.io.ReduceSideJoinWritable;
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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Auther : guojianmin
 * @Date : 2019/5/16 08:05
 * @Description : 需求6 map端Join和分布式缓存(缓存单个文件或者缓存多个文件的压缩包 .zip)
 *
 *  将需求5 得到的结果，与地域关系表进行关联，关联出地域名称
 * 等价sql：select a.date,b.area_name,a.pv,a.click,a.user_count
 *              from report as a join area_info_rel as b
 *              on a.area_id=b.area_id
 * 由于地域关系表很小，没必要在reduce里处理，可以采用hadoop 在MapReduce中提供的分布式缓存的方式
 * 这样做的好处是，利用分布式缓存不需要进行网络传输，提高性能
 *
 * 实现思路：
 * map阶段：根据文件名称区分文件（表）,然后通过每一行数据的分隔符，根据不同文件取到不同的，需要的数据
 *         然后将<area_id,Entity> 输出到reduce中（将相同地域编码的数据，由同一个reduce进行处理）
 * reduce阶段：area_id相同的 调用一次reduce处理 进行整合统计，将数据整理成结果表的一条数据，放在Entity中，然后输出<Null，Entity>
 *
 */
public class MapSideJoinMRJobNew extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //1.配置job
        Configuration conf = this.getConf();
        Job job = null;

        //2.创建job
        job = Job.getInstance(conf);
        job.setJarByClass(MapSideJoinMRJobNew.class);//设置通过主类来获取job

        //3.给job设置执行流程
        //3.1 HDFS中需要处理的文件路径(多文件路径)
        Path path = new Path(args[0]);
        //job添加输入路径
        FileInputFormat.addInputPath(job, path);

        //3.2设置map执行流程
        job.setMapperClass(MapSideJoinMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);//设置map输出key的类型
        job.setMapOutputValueClass(Text.class);//设置map输出value的类型

        //3.2设置reduce执行流程(map阶段缓存，不需要reduce)

        //设置ReduceTasks 个数为0 因为默认有一个
        job.setNumReduceTasks(0);//硬编码，不友好，建议通过传参的方式实现

        job.addCacheFile(new URI(args[2]));//缓存单个HDFS中的文件到task运行节点的本地工作目录
//        job.addCacheArchive(new URI(args[2]));//缓存多个HDFS中的文件包（.zip）到task运行节点的本地工作目录

//        job.setPartitionerClass(TerminalTypePartitioner.class);//设置自定义的 Partitioner

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
    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        Map<String,String> areaMap = new HashMap<>();//用于存储地域id和地域名称的关系映射关系
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //缓存单个文件
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("area_info.txt")));
            //缓存多个文件的压缩文件包
            //BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("area_info.zip/area_info.txt")));
            String line = "";
            while ((line = br.readLine()) != null){
                String[] areaInfos = line.split("\t");
                areaMap.put(areaInfos[0],areaInfos[1]);
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split("\t");
            String areaId = fields[1];
            String areaName = areaMap.get(areaId);
            String outputValue = fields[0] + "\t"+ areaName  + "\t" + fields[2] + "\t" + fields[3] + "\t" + fields[4];
            context.write(NullWritable.get(),new Text(outputValue));
//            Thread.sleep(5000);//等待5秒钟，为了查看结果
        }
    }


    public static void main(String[] args) {

        //用于本地测试
        if (args.length == 0){
            args = new String[]{
                "hdfs://ns/mr_project/log_analysis/output5/ReduceSideJoin/part-r-00000",
                "hdfs://ns/mr_project/log_analysis/output6/MapSideJoin"
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
            int status = ToolRunner.run(conf, new MapSideJoinMRJobNew(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //hadoop jar xxxx.jar mainclass -Dmapreduce.job.reduce=3    设置reduce的数量

    }
}
