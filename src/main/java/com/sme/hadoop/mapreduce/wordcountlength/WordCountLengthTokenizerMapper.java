package com.sme.hadoop.mapreduce.wordcountlength;

import static com.sme.hadoop.util.TextSplitter.splitAphaBetic;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WordCountLength mapper.
 */
class WordCountLengthTokenizerMapper extends Mapper<Object, Text, Text, LongWritable>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountLengthTokenizerMapper.class);

    private static final Text COUNT = new Text("count");
    private static final Text LENGTH = new Text("length");
    private static final LongWritable ONE = new LongWritable(1);

    private final LongWritable wordLength = new LongWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens())
        {
            String nextToken = itr.nextToken();
            LOGGER.debug("Process {} token", nextToken);

            String alphaBeticWord = splitAphaBetic(nextToken);
            wordLength.set(alphaBeticWord.length());
            context.write(LENGTH, wordLength);
            context.write(COUNT, ONE);
        }
    }
}
