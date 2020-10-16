package com.sme.hadoop.writer;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A JSON output format handler for Hadoop MapReduce jobs.
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class AJsonOutputFormat<K, V> extends FileOutputFormat<K, V>
{
    private final ObjectMapper mapper;

    public AJsonOutputFormat()
    {
        this.mapper = createMapper();
    }

    /**
     * Convert the key field.
     *
     * @param key The Writable key;
     * @return Returns String key representation.
     */
    protected abstract String convertKey(K key);

    /**
     * Convert the value field.
     *
     * @param key The key value;
     * @param value The Writable value;
     * @return Returns JsonNode value.
     */
    protected abstract JsonNode convertValue(K key, V value);

    /**
     * Merge json if the field already exists.
     *
     * @param left The existing value;
     * @param right The new value;
     * @return Retuns the value to persist.
     */
    protected JsonNode merge(JsonNode left, JsonNode right)
    {
        return right;
    }

    @Override
    public final RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException
    {
        Configuration conf = job.getConfiguration();

        String ext = conf.get("json.ext", ".json");
        String name = conf.get("json.file", "json_output.json");

        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(job);

        Path file = getOutputPath(job, conf, ext, name, committer);

        FileSystem fs = file.getFileSystem(conf);

        try
        {
            FSDataOutputStream out = fs.create(file, false);
            return new JsonOutputWriter(out);
        }
        catch (Exception e)
        {
            throw new RuntimeException("TODO", e);
        }
    }

    private Path getOutputPath(TaskAttemptContext job, Configuration conf, String ext, String name, FileOutputCommitter committer) throws IOException
    {
        boolean isUniqueFile = Boolean.valueOf(conf.get("json.file.unique", "true"));
        return isUniqueFile ? new Path(committer.getWorkPath(), getUniqueFile(job, name, ext)) : new Path(committer.getWorkPath(), name + ext);
    }

    private ObjectMapper createMapper()
    {
        return new ObjectMapper();
    }

    /**
     * {@link RecordWriter} in JSON format.
     */
    private class JsonOutputWriter extends RecordWriter<K, V>
    {
        private final DataOutputStream out;
        private final ObjectNode json;

        JsonOutputWriter(DataOutputStream out)
        {
            this.json = mapper.createObjectNode();
            this.out = out;
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException
        {
            String field = convertKey(key);

            JsonNode left = this.json.path(field);
            JsonNode right = convertValue(key, value);

            this.json.set(field, left.isMissingNode() ? right : merge(left, right));
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
        {
            this.out.write(mapper.writeValueAsBytes(this.json));
            this.out.close();
        }
    }
}
