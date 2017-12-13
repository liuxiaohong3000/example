package Sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//排序
public class SortHot extends WritableComparator{

    public SortHot() {
        super(KeyPari.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        //排序规则
        KeyPari k1=(KeyPari) a;
        KeyPari k2=(KeyPari) b;

        int res = Integer.compare(k1.getYear(),k2.getYear());
        if(res!=0){
            return res;
        }

        return -Integer.compare(k1.getHot(),k2.getHot());
    }

}
