package org.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedReducer extends Reducer<Text, Text, Text,Text> {
    private Text result = new Text();
//    private ArrayList<String> Columns=new ArrayList<String>();
//    private ArrayList<String> Values=new ArrayList<String>();
    public InvertedReducer() {
    }

    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        Iterator var5 = values.iterator();
        while(var5.hasNext()) {
            Text value = (Text)var5.next();
            builder.append(value.toString());
            builder.append(";");
            // 得到 file_02 与 3
//            String[] strings=value.toString().split(":");
//            Columns.add(strings[0]);
//            Values.add(strings[1]);
        }
//        InvertedDriver.insert(key.toString(),Columns,Values);
//        Columns.clear();
//        Values.clear();
        this.result.set(builder.toString());
        context.write(key, this.result);
    }
}
