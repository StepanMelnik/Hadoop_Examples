package com.sme.hadoop.mapreduce.wordcountlength;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.hadoop.writer.LongJsonOutputFormat;

/**
 * WordCount job.
 */
class WordCountLengthJob extends Configured implements Tool
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountLengthJob.class);

    @Override
    public int run(String[] args) throws Exception
    {
        // CSOFF
        if (args.length != 5)
        {
            new IllegalArgumentException("WordCountLengthJob expects the following arguments: InputFile, OutputFolder, json.file.unique, json.file, json.ext");
        }

        String input = args[0];
        String output = args[1];
        String jsonFileUnique = args[2];
        String jsonFile = args[3];
        String jsonExt = args[4];
        // CSON

        Configuration configuration = new Configuration();
        configuration.set("json.file.unique", jsonFileUnique);
        configuration.set("json.file", jsonFile);
        configuration.set("json.ext", jsonExt);

        Job job = Job.getInstance(configuration, "WordCount");
        job.setJarByClass(WordCountLengthJob.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(LongJsonOutputFormat.class);

        job.setMapperClass(WordCountLengthTokenizerMapper.class);
        job.setCombinerClass(WordCountLengthSumReducer.class);
        job.setReducerClass(WordCountLengthSumReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        LOGGER.debug(job.isSuccessful() ? "Job was successful" : "Job was not successful");

        return returnValue;
    }
}
