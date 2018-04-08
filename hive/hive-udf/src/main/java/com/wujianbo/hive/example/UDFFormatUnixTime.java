package com.wujianbo.hive.example;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wujianbo on 2018/4/8.
 */
@Description(
        name = "format_unixtime",
        value = "_FUNC_(unix_time) - returns unix_time in the 'yyyy-MM-dd HH:mm:ss' format, '0' for null or 0.",
        extended = "SELECT _FUNC_(1474300862) limit 1"
)
public class UDFFormatUnixTime extends UDF {
    private SimpleDateFormat formatter;

    private Text result = new Text();

    public UDFFormatUnixTime() {

    }

    /**
     * Convert UnixTime to a string format.
     *
     * @param unixtime
     *          The number of seconds from 1970-01-01 00:00:00
     * @return a String in default format specified.
     */
    public Text evaluate(LongWritable unixtime) {
        if (unixtime == null || unixtime.get() == 0) {
            result.set("0");
            return result;
        }
        else {
            return eval(unixtime.get());
        }
    }

    /**
     * Internal evaluation function given the seconds from 1970-01-01 00:00:00 and
     * the output text format.
     *
     * @param unixtime
     *          seconds of type long from 1970-01-01 00:00:00
     *
     * @return elapsed time in the format 'yyyy-MM-dd HH:mm:ss'.
     */
    private Text eval(long unixtime) {

        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // convert seconds to milliseconds
        Date date = new Date(unixtime * 1000L);
        result.set(formatter.format(date));
        return result;
    }

    //测试的main方法
    public static void main(String[] args) throws Exception{
        System.out.println(System.currentTimeMillis());
    }
}
