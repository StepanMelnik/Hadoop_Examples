package com.sme.hadoop.mapreduce.multivalues;

import static java.lang.String.format;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MultiValues reducer.
 */
class MultiValuesReducer extends Reducer<Text, MapWritable, Text, MapWritable>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiValuesReducer.class);

    private Text result;
    private MapWritable map;

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
    {
        StringBuilder builder = new StringBuilder("[");

        MapWritable currentValue = context.getCurrentValue();
        for (MapWritable val : values)
        {
            //Text fileName = (Text) val.get(new Text(MultiValuesConstant.FILE_NAME));  // only for multiple files reading
            Text line = (Text) val.get(new Text(MultiValuesConstant.LINE));
            Text offset = (Text) val.get(new Text(MultiValuesConstant.OFFSET));

            if (line != null && offset != null) // null values on error
            {
                builder.append(format("{\"lineOffset\":%s,\"charOffset\":%s},", line.toString(), offset.toString()));
            }
        }

        if (builder.length() > 2)
        {
            builder.delete(builder.length() - 1, builder.length());
        }

        builder.append("]");

        result = new Text(builder.toString());

        if (builder.toString().equals("[]"))
        {
            map = currentValue;
        }
        else
        {
            map = new MapWritable();
            map.put(key, result);
        }

        LOGGER.debug("Reduce {} key with {} value", key.toString(), currentValue.toString());
        context.write(key, map);
    }
}
