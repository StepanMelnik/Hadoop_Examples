package com.sme.hadoop.writer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Prepares JSON output with {@link LongWritable}.
 */
public class LongJsonOutputFormat extends AJsonOutputFormat<Text, LongWritable>
{
    @Override
    protected String convertKey(Text key)
    {
        return key.toString();
    }

    @Override
    protected JsonNode convertValue(Text key, LongWritable value)
    {
        return JsonNodeFactory.instance.numberNode(value.get());
    }

    @Override
    protected JsonNode merge(JsonNode left, JsonNode right)
    {
        return JsonNodeFactory.instance.numberNode(left.asLong(0) + right.asLong(0));
    }
}
