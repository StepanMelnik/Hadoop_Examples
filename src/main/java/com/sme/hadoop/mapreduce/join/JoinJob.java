package com.sme.hadoop.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.hadoop.writer.TextTextJsonOutputFormat;

/**
 * Join job.
 */
class JoinJob extends Configured implements Tool
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinJob.class);

    @Override
    public int run(String[] args) throws Exception
    {
        // CSOFF
        if (args.length != 6)
        {
            new IllegalArgumentException("JoinJob expects the following arguments: Department InputFile, Employee InputFile, OutputFolder, json.file.unique, json.file, json.ext");
        }

        String departmentInput = args[0];
        String employeeInput = args[1];
        String output = args[2];
        String jsonFileUnique = args[3];
        String jsonFile = args[4];
        String jsonExt = args[5];
        // CSON

        Configuration configuration = new Configuration();
        configuration.set("json.file.unique", jsonFileUnique);
        configuration.set("json.file", jsonFile);
        configuration.set("json.ext", jsonExt);

        Job job = Job.getInstance(configuration, "Join");
        job.setJarByClass(JoinJob.class);

        MultipleInputs.addInputPath(job, new Path(departmentInput), TextInputFormat.class, DepartmentMapper.class);
        MultipleInputs.addInputPath(job, new Path(employeeInput), TextInputFormat.class, EmployeeMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextTextJsonOutputFormat.class);

        job.setReducerClass(JoinReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        LOGGER.debug(job.isSuccessful() ? "Job was successful" : "Job was not successful");

        return returnValue;
    }
}
