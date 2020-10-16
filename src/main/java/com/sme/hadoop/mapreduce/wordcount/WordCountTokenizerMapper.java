package com.sme.hadoop.mapreduce.wordcount;

import static com.sme.hadoop.util.TextSplitter.splitAphaBetic;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WordCount mapper.
 */
class WordCountTokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTokenizerMapper.class);
    private static final IntWritable ONE = new IntWritable(1);

    private final Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens())
        {
            String nextToken = itr.nextToken();
            LOGGER.debug("Process {} token", nextToken);

            String alphaBeticWord = splitAphaBetic(nextToken);
            word.set(alphaBeticWord);
            context.write(word, ONE);
        }
    }
}
