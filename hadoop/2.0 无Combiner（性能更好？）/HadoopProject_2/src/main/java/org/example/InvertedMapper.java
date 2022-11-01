package org.example;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyInfo = new Text();
    private Text valueInfo = new Text();
    private FileSplit split;

    public InvertedMapper() {
    }

    public void map(Object key, Text value,Context context) throws IOException, InterruptedException {
        this.split = (FileSplit)context.getInputSplit();
        String fileName = this.split.getPath().getName();
        String[] itr = value.toString().split("\\s");

        for(int x = 1; x < itr.length; ++x) {
            this.keyInfo.set(itr[x] + ":" + fileName);
            this.valueInfo.set("1");
            context.write(this.keyInfo, this.valueInfo);
        }
    }
}

