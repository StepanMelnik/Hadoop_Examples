package com.sme.hadoop.mapreduce.wordcountlength;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount sum reducer.
 */
class WordCountLengthSumReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
    private final LongWritable result = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for (LongWritable val : values)
        {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
