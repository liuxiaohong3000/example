package Sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//分组

public class GroupHot extends WritableComparator{

    public GroupHot() {
        super(KeyPari.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //排序规则
        KeyPari k1=(KeyPari) a;
        KeyPari k2=(KeyPari) b;
        return Integer.compare(k1.getYear(),k2.getYear());
    }

}
