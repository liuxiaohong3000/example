import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DelRepeat {

    // map将输入中的value复制到输出数据的key上，并直接输出

    public static class Map extends Mapper<Object, Text, Text, Text> {

        private static Text line = new Text();// 每行数据

        // 实现map函数
        public void map(Object key, Text value, Context context)

                throws IOException, InterruptedException {

            line = value;

            context.write(line, new Text(""));
        }
    }

    // reduce将输入中的key复制到输出数据的key上，并直接输出    这是数据区重的思想
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        // 实现reduce函数

        public void reduce(Text key, Iterable<Text> values, Context context)

                throws IOException, InterruptedException {

            context.write(key, new Text(""));

        }

    }
    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
    public static void main(String[] args) throws Exception {

        File outputFile=new File("E:/WORK_SPACE/example/hadoop/output");
        deleteDir(outputFile);
        JobConf conf = new JobConf(WordCount.class);
        String[] path = new String[2];
        /*path[0] = "hdfs://ip:9001/user/hadoop/input/";
        path[1] = "hdfs://ip:9001/user/hadoop/output";*/
        path[0] = "input/hello.txt";
        path[1] = "output";
        String[] otherArgs = new GenericOptionsParser(conf, path).getRemainingArgs();
        //set maprduce job name
        Job job = new Job(conf, "Data Deduplication");

        job.setJarByClass(DelRepeat.class);

        // 设置Map、Combine和Reduce处理类

        job.setMapperClass(Map.class);

        job.setCombinerClass(Reduce.class);

        job.setReducerClass(Reduce.class);

        // 设置输出类型

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}