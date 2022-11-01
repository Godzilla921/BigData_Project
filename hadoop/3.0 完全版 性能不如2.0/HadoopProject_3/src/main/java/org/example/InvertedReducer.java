package org.example;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class InvertedReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          TableReducer<Text, Text, ImmutableBytesWritable>.Context context)
            throws IOException, InterruptedException {
        // 传入的数据格式为 mapper [file_01:4 file_03:5]
        // 设置rowKey
        Put put=new Put(key.toString().getBytes());
        for (Text value:values
             ) {
            String str=value.toString();
            int index=str.indexOf(":");
            // 得到  column和value
            String Column=str.substring(0,index);
            String Value=str.substring(index+1);
            // columnFamily column value
            put.addColumn(InvertedDriver.columnFamily.get(0).getBytes(),Column.getBytes(),Value.getBytes());
            context.write(null,put);
        }
    }
}
