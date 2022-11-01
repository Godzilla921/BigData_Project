package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class InvertedCombiner extends Reducer<Text, Text, Text, Text> {
    private Text keyInfo = new Text();
    private Text valueInfo = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (Text value:values){
            sum+=Integer.parseInt(value.toString());
        }
        String str=key.toString();
        int index=str.indexOf(":");
        this.keyInfo.set(str.substring(0,index));
        this.valueInfo.set(str.substring(index+1) + ":" + sum);
        context.write(this.keyInfo, this.valueInfo);
    }
}
