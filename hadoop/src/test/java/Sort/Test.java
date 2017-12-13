package Sort;

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test {

    private static SimpleDateFormat SDF=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws FileNotFoundException {

        String line="1949-10-01 14:23:01  34°C";
        String[] ss = line.split("\t");
        if(ss.length==2){
            try {
                Date date = SDF.parse(ss[0]);

                Calendar c=Calendar.getInstance();
                c.setTime(date);
                int year=c.get(1);
                String hot=ss[1].substring(0,ss[1].lastIndexOf("°C"));
                KeyPari keyPari=new KeyPari();
                keyPari.setYear(year);
                keyPari.setHot(Integer.parseInt(hot));

                System.out.println(keyPari);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }



    }

}
