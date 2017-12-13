import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FactoryAddress2 {
    public static final String LEFT="L";
    public static final String RIGHT="R";
    // map将输入中的value复制到输出数据的key上，并直接输出

    public static class Map extends Mapper<Object, Text, Text, Text> {

        // 实现map函数
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String sourceStr=value.toString();
            //忽略标题
            if(!sourceStr.contains("address")){
                //内容类型
                String Left=LEFT;
                String[] sourceArray=sourceStr.split("#");
                char firstChar=sourceArray[0].charAt(0);
                //解析第一个字符，判断属于那种类型
                if(firstChar>='0' && firstChar <='9'){
                    Left=RIGHT;
                }
                if(Left.equals(LEFT)) {
                    context.write(new Text(sourceArray[1]),new Text(LEFT+sourceArray[0]));
                }else{
                    context.write(new Text(sourceArray[0]),new Text(RIGHT+sourceArray[1]));
                }
            }
        }
    }

    // reduce将输入中的key复制到输出数据的key上，并直接输出    这是数据区重的思想
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        // 实现reduce函数

        public void reduce(Text key, Iterable<Text> values, Context context)

                throws IOException, InterruptedException {

            List<String> leftList=new ArrayList<String>();
            List<String> rightList=new ArrayList<String>();
            Iterator iterator=values.iterator();
            while(iterator.hasNext()){
                String value = iterator.next().toString();
                String firstChar=value.substring(0,1);
                if(LEFT.equals(firstChar)){
                    leftList.add(value.substring(1));
                }else{
                    rightList.add(value.substring(1));
                }
                System.out.println(key+"----"+value);
            }
            for(String left : leftList){
                for (String right : rightList){
                    context.write(new Text(left),new Text(right));
                }
            }

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
        JobConf conf = new JobConf();
        String[] path = new String[2];
        /*path[0] = "hdfs://ip:9001/user/hadoop/input/";
        path[1] = "hdfs://ip:9001/user/hadoop/output";*/
        path[0] = "input/address2";
        path[1] = "output";
        String[] otherArgs = new GenericOptionsParser(conf, path).getRemainingArgs();
        //set maprduce job name
        Job job = new Job(conf, "Data Deduplication");

        job.setJarByClass(FactoryAddress2.class);

        // 设置Map、Combine和Reduce处理类

        job.setMapperClass(Map.class);

        //有问题，可能是因为没有设置合并的算法，导致最终数据没有合并,可通过注释和开启测试
        //job.setCombinerClass(Reduce.class);

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
