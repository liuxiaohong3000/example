package Sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FristPartition extends Partitioner<KeyPari,Text>{

    //自定义分区
    @Override
    public int getPartition(KeyPari key, Text value, int num) {
        return (key.getYear()*127)%num;
    }

}
