package bd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * @Auther : guojianmin
 * @Date : 2019/5/14 7:40
 * @Description : HDFS的 java api来读 写文件
 */
public class HDFSClient {

    /**
     * 获取HDFS文件系统对象
     * @return
     * @throws IOException
     */
    private FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);//创建hdfs文件系统对象
        return fs;
    }

    /**
     * 通过HDFS的 java api来读取hdfs中的文件
     * @param hdfsFilePath
     */
    public void readHDFSFile(String hdfsFilePath){
        BufferedReader reader = null;
        FSDataInputStream fsDataInputStream = null;
        try {
            Path path = new Path(hdfsFilePath);
            fsDataInputStream = this.getFileSystem().open(path);//根据path创建FSDataInputStream输入流对象
            reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String line = "";
            while((line = reader.readLine()) != null){
                System.out.println(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (fsDataInputStream != null){
                    fsDataInputStream.close();
                }
                if (reader != null){
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 将本地文件内容写入到hdfs
     * @param localFilePath
     * @param hdfsFilePath
     */
    public void writeHDFSFile(String localFilePath,String hdfsFilePath) {
        FSDataOutputStream fsDataOutputStream = null;
        FileInputStream fileInputStream = null;
        Path path = new Path(hdfsFilePath);
        try {
            fsDataOutputStream = this.getFileSystem().create(path);
            fileInputStream = new FileInputStream(new File(localFilePath));
            IOUtils.copyBytes(fileInputStream,fsDataOutputStream,4096,false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (fsDataOutputStream != null){
                    fsDataOutputStream.close();
                }
                if (fileInputStream != null){
                    fileInputStream.close();
                }
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        String hdfsFilePath = "hdfs://ns/input/wc/wc-test1.txt";
        HDFSClient hdfsClient = new HDFSClient();
        String localFilePath = "D:/user/wc-test1.txt";
        hdfsClient.writeHDFSFile(localFilePath,hdfsFilePath);//写入文件
        hdfsClient.readHDFSFile(hdfsFilePath);//读取文件

    }
}
