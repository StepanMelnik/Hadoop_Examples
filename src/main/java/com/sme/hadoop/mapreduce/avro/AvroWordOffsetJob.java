package com.sme.hadoop.mapreduce.avro;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.hadoop.writer.AvroJsonOutputFormat;

/**
 * AvroWordOffset job.
 */
class AvroWordOffsetJob extends Configured implements Tool
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroWordOffsetJob.class);

    @Override
    public int run(String[] args) throws Exception
    {
        // CSOFF
        if (args.length != 5)
        {
            new IllegalArgumentException("AvroWordOffsetJob expects the following arguments: InputFile, OutputFolder, json.file.unique, json.file, json.ext");
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
        configuration.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(configuration, "AvroWordOffsetJob");
        job.setJarByClass(AvroWordOffsetJob.class);

        FileInputFormat.addInputPath(job, new Path(input));
        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(configuration).delete(outputPath, true);   // important option to avoid "Cannot create a file when that file already exists." error

        //AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, AvroWordOffsetConstant.AVRO_WORD_OFFSET_SCHEMA);
        //AvroJob.setOutputKeySchema(job, AvroWordOffsetConstant.AVRO_WORD_OFFSET_SCHEMA);
        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(MapWritable.class);

        job.setInputFormatClass(AvroWordOffsetInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(AvroJsonOutputFormat.class);

        job.setMapperClass(AvroWordOffsetMapper.class);
        job.setCombinerClass(AvroWordOffsetReducer.class);
        job.setReducerClass(AvroWordOffsetReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        LOGGER.debug(job.isSuccessful() ? "Job was successful" : "Job was not successful");

        return returnValue;
    }
}
