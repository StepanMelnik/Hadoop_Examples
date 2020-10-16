package com.sme.hadoop.writer;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Prepares JSON output with {@link TextWritable} key and {@link MapWritable} value.
 */
public class TextMapJsonOutputFormat extends AJsonOutputFormat<Text, MapWritable>
{
    @Override
    protected String convertKey(Text key)
    {
        return key.toString();
    }

    @Override
    protected JsonNode convertValue(Text key, MapWritable value)
    {
        return JsonNodeFactory.instance.textNode(value.get(key).toString());
    }

    @Override
    protected JsonNode merge(JsonNode left, JsonNode right)
    {
        return JsonNodeFactory.instance.textNode(left.asText() + right.asText());
    }
}
