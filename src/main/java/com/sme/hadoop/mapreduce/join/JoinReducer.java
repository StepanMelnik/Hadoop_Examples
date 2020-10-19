package com.sme.hadoop.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Join reducer.
 */
class JoinReducer extends Reducer<Text, Text, Text, Text>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinReducer.class);

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        StringBuilder employeeBuilder = new StringBuilder("");
        String deptartment = "";

        for (Text value : values)
        {
            if (value.toString().startsWith("employee"))
            {
                String employee = value.toString().split(":")[1];
                LOGGER.debug("Process {} key with {} employee value", key, employee);
                employeeBuilder.append(employee).append(",");
            }
            else
            {
                deptartment = value.toString().split(":")[1];
                LOGGER.debug("Process {} key with {} department value", key, deptartment);
            }
        }
        String merge = deptartment + ":" + employeeBuilder.toString();
        context.write(new Text(key.toString()), new Text(merge));
    }
}
