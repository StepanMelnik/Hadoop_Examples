package com.sme.hadoop.writer;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.sme.hadoop.mapreduce.avro.AvroWordOffsetConstant;

/**
 * Prepares JSON output with {@link TextWritable} key and {@link AvroValue} value.
 */
public class AvroJsonOutputFormat extends AJsonOutputFormat<Text, AvroValue<GenericRecord>>
{
    @Override
    protected String convertKey(Text key)
    {
        return key.toString();
    }

    @Override
    protected JsonNode convertValue(Text key, AvroValue<GenericRecord> value)
    {
        GenericRecord record = value.datum();
        Object line = record.get(AvroWordOffsetConstant.RESULT);

        return JsonNodeFactory.instance.textNode(line.toString());
    }

    @Override
    protected JsonNode merge(JsonNode left, JsonNode right)
    {
        return JsonNodeFactory.instance.textNode(left.asText() + right.asText());
    }
}
