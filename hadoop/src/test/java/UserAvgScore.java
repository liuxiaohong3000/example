import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * wordcount 的简单例子
 *
 * @author hadoop
 */

public class UserAvgScore {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,"\n");
            while (tokenizer.hasMoreTokens()) {
                StringTokenizer subtokenizer = new StringTokenizer(tokenizer.nextToken());
                String name=subtokenizer.nextToken();
                String score=subtokenizer.nextToken();
                output.collect(new Text(name), new IntWritable(Integer.parseInt(score)));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sum = 0;
            int count=0;
            while (values.hasNext()) {
                sum += values.next().get();
                count++;
            }
            output.collect(key, new IntWritable(sum/count));
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
        JobConf conf = new JobConf(UserAvgScore.class);
        String[] path = new String[2];
        /*path[0] = "hdfs://ip:9001/user/hadoop/input/";
        path[1] = "hdfs://ip:9001/user/hadoop/output";*/
        path[0] = "input/userScore.txt";
        path[1] = "output";
        String[] otherArgs = new GenericOptionsParser(conf, path).getRemainingArgs();
        conf.setJobName("worscount");
        conf.setUser("hadoop");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));
        JobClient.runJob(conf);
    }
}