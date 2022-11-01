package org.example;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class InvertedReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    public InvertedReducer() {
    }
    // 传入的数据格式为 mapper:file_02 1
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // 传入的格式为 mapper:file_04 [1,1,1,1]
        // deal with content
        int sum=0;
        for (Text value:values
             ) {
            sum+=Integer.parseInt(value.toString());
        }
        String[] strings=key.toString().split(":");
        // 设置每一行的键
        Put put=new Put(strings[0].getBytes());
        put.addColumn(InvertedDriver.columnFamily.get(0).getBytes(),strings[1].getBytes(),String.valueOf(sum).getBytes());
        context.write(null,put);
    }
}
