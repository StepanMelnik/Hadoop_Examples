package com.sme.hadoop.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Employee mapper.
 */
public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable line, Text value, Context context) throws IOException, InterruptedException
    {
        String[] words = value.toString().split(",");
        if (words.length == 3)
        {
            context.write(new Text(words[1]), new Text("employee:" + words[2]));
        }
    }
}
