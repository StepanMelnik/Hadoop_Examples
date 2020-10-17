package com.sme.hadoop.mapreduce.avro;

import static com.sme.hadoop.mapreduce.avro.AvroWordOffsetConstant.FILE_NAME;
import static com.sme.hadoop.mapreduce.avro.AvroWordOffsetConstant.LINE;
import static com.sme.hadoop.mapreduce.avro.AvroWordOffsetConstant.OFFSET;
import static com.sme.hadoop.mapreduce.avro.AvroWordOffsetConstant.RESULT;
import static com.sme.hadoop.util.TextSplitter.splitAphaBetic;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.hadoop.util.TextSplitter;

/**
 * AvroWordOffset mapper.
 */
class AvroWordOffsetMapper extends Mapper<AvroWordOffset, Text, Text, AvroValue<GenericRecord>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroWordOffsetMapper.class);
    private static Set<String> FILTER = new HashSet<>(asList("James", "John", "Robert", "Michael"));

    private final Text word = new Text();
    private final GenericRecord record = new GenericData.Record(AvroWordOffsetConstant.AVRO_WORD_OFFSET_SCHEMA);

    @Override
    public void map(AvroWordOffset key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();

        LOGGER.debug("Process {} line", line);

        record.put(FILE_NAME, key.getFileName());
        record.put(LINE, key.getLine());
        record.put(RESULT, "");

        long charOffset = key.getOffset();

        for (ImmutablePair<String, String> pair : TextSplitter.splitWithSpaces(line))
        {
            LOGGER.trace("Process \"{}\" word in the {} thread", pair.getLeft(), Thread.currentThread().getName());

            String space = pair.getRight();
            String wordWithNonWordChars = StringUtils.substringAfter(pair.getLeft(), space);
            String alphaBeticWord = splitAphaBetic(wordWithNonWordChars);

            if (FILTER.contains(alphaBeticWord))
            {
                long wordCharOffset = charOffset + space.length();
                record.put(OFFSET, wordCharOffset);
                word.set(alphaBeticWord);

                LOGGER.debug("Processed {} word with {} map", alphaBeticWord, record);
                context.write(word, new AvroValue<>(record));
            }
            charOffset = charOffset + wordWithNonWordChars.length() + space.length();
        }
    }
}
