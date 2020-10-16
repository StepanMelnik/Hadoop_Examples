package com.sme.hadoop.mapreduce.multivalues;

import static com.sme.hadoop.util.TextSplitter.splitAphaBetic;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.hadoop.util.TextSplitter;

/**
 * MultiValues mapper.
 */
class MultiValuesMapper extends Mapper<MultiValuesWordOffset, Text, Text, MapWritable>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiValuesMapper.class);
    private static Set<String> FILTER = new HashSet<>(asList("James", "John", "Robert", "Michael"));

    private final MapWritable map = new MapWritable();
    private final Text word = new Text();

    @Override
    public void map(MultiValuesWordOffset key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();

        LOGGER.debug("Process {} line", line);

        map.put(new Text(MultiValuesConstant.FILE_NAME), new Text(key.getFileName()));
        map.put(new Text(MultiValuesConstant.LINE), new Text(String.valueOf(key.getLine())));

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

                map.put(new Text(MultiValuesConstant.OFFSET), new Text(String.valueOf(wordCharOffset)));
                word.set(alphaBeticWord);

                LOGGER.debug("Processed {} word with {} map", alphaBeticWord, map);
                context.write(word, map);
            }
            charOffset = charOffset + wordWithNonWordChars.length() + space.length();
        }
    }
}
