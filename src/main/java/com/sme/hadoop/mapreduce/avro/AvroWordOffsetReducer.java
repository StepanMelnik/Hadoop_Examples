package com.sme.hadoop.mapreduce.avro;

import static com.sme.hadoop.mapreduce.avro.AvroWordOffsetConstant.FILE_NAME;
import static com.sme.hadoop.mapreduce.avro.AvroWordOffsetConstant.LINE;
import static com.sme.hadoop.mapreduce.avro.AvroWordOffsetConstant.OFFSET;
import static com.sme.hadoop.mapreduce.avro.AvroWordOffsetConstant.RESULT;
import static java.lang.String.format;

import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AvroWordOffset reducer.
 */
class AvroWordOffsetReducer extends Reducer<Text, AvroValue<GenericRecord>, Text, AvroValue<GenericRecord>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroWordOffsetReducer.class);

    private AvroValue<GenericRecord> avroValue;

    @Override
    public void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException
    {
        StringBuilder builder = new StringBuilder("[");
        AvroValue<GenericRecord> currentValue = context.getCurrentValue();

        if (StringUtils.isBlank(currentValue.datum().get(RESULT).toString()))
        {
            for (AvroValue<GenericRecord> value : values)
            {
                GenericRecord record = value.datum();
                Object line = record.get(LINE);
                Object offset = record.get(OFFSET);

                builder.append(format("{\"lineOffset\":%s,\"charOffset\":%s},", line.toString(), offset.toString()));
            }

            if (builder.length() > 2)
            {
                builder.delete(builder.length() - 1, builder.length());
            }

            builder.append("]");
            avroValue = new AvroValue<>(createGenericRecord(currentValue.datum(), builder.toString()));
        }
        else
        {
            avroValue = currentValue;
        }

        LOGGER.debug("Reduce {} key with {} value", key.toString(), avroValue.toString());
        context.write(key, avroValue);
    }

    private GenericRecord createGenericRecord(GenericRecord value, String result)
    {
        GenericRecord record = new GenericData.Record(AvroWordOffsetConstant.AVRO_WORD_OFFSET_SCHEMA);
        record.put(FILE_NAME, value.get(FILE_NAME));
        record.put(LINE, value.get(LINE));
        record.put(OFFSET, value.get(OFFSET));
        record.put(RESULT, result);
        return record;
    }
}
