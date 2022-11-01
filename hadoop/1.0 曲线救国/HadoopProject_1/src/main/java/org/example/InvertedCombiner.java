package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InvertedCombiner extends Reducer<Text, Text, Text, Text> {
    private Text keyInfo = new Text();
    private Text valueInfo = new Text();

    public InvertedCombiner() {
    }

    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int sum = 0;

        Text value;
        for(Iterator var5 = values.iterator(); var5.hasNext(); sum += Integer.parseInt(value.toString())) {
            value = (Text)var5.next();
        }

        String[] words = key.toString().split(":");
        this.keyInfo.set(words[0]);
        this.valueInfo.set(words[1] + ":" + sum);
        context.write(this.keyInfo, this.valueInfo);
    }
}
