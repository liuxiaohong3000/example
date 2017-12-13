package Sort;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//运行类
public class RunJob {


    private static SimpleDateFormat SDF=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //map
    static class HotMapper extends Mapper<LongWritable, Text, KeyPari, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line=value.toString();
            String[] ss = line.split("#");
            if(ss.length==3){
                //等于2处理
                try {
                    Date date = SDF.parse(ss[0]);

                    Calendar c=Calendar.getInstance();
                    c.setTime(date);
                    int year=c.get(1);
                    String hot=ss[1];
                    KeyPari keyPari=new KeyPari();
                    keyPari.setYear(year);
                    keyPari.setHot(Integer.parseInt(hot));

                    context.write(keyPari, value);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }


    //reduce
    static class HotReduce extends Reducer<KeyPari,Text,KeyPari,Text>{

        @Override
        protected void reduce(KeyPari kp, Iterable<Text> values,
                              Reducer<KeyPari, Text, KeyPari, Text>.Context context)
                throws IOException, InterruptedException {
            for(Text t:values){
                context.write(kp, t);
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
    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        File outputFile=new File("E:/WORK_SPACE/example/hadoop/output");
        deleteDir(outputFile);
        Configuration cfg = new Configuration();

        try {
            Job job = new Job(cfg);
            job.setJobName("hot");
            job.setJarByClass(RunJob.class);
            job.setMapperClass(HotMapper.class);
            job.setReducerClass(HotReduce.class);
            job.setOutputKeyClass(KeyPari.class);
            job.setOutputValueClass(Text.class);

            job.setNumReduceTasks(6);
            job.setPartitionerClass(FristPartition.class);
            job.setSortComparatorClass(SortHot.class);
            job.setGroupingComparatorClass(GroupHot.class);


            FileInputFormat.addInputPath(job,new Path("input/sort.txt"));
            FileOutputFormat.setOutputPath(job,new Path("output"));

            System.exit(job.waitForCompletion(true)?0:1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



}
