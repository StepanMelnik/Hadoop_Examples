package com.sme.hadoop.mapreduce.multivalues;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * FileInptFormat to fetch records line by line .
 */
public class MultiValuesInputFormat extends CombineFileInputFormat<MultiValuesWordOffset, Text>
{
    @Override
    public RecordReader<MultiValuesWordOffset, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException
    {
        return new CombineFileRecordReader<>((CombineFileSplit) split, context, CombineFileLineRecordReader.class);
    }

    /**
     * RecordReader is responsible from extracting records from a chunk of the CombineFileSplit.
     */
    private static class CombineFileLineRecordReader extends RecordReader<MultiValuesWordOffset, Text>
    {
        private long startOffset;
        private final long end;
        private long currentPosition;
        private long currentLine = 1;
        private final FileSystem fileSystem;
        private final Path path;
        private MultiValuesWordOffset key;
        private Text value;

        private final FSDataInputStream fileIn;
        private final LineReader reader;

        @SuppressWarnings("unused")
        CombineFileLineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException
        {
            this.path = split.getPath(index);
            fileSystem = this.path.getFileSystem(context.getConfiguration());
            this.startOffset = split.getOffset(index);
            this.end = startOffset + split.getLength(index);
            boolean skipFirstLine = false;

            //open the file
            fileIn = fileSystem.open(path);
            if (startOffset != 0)
            {
                skipFirstLine = true;
                --startOffset;
                fileIn.seek(startOffset);
            }
            reader = new LineReader(fileIn);
            if (skipFirstLine)
            {
                // skip first line and re-establish "startOffset"
                startOffset += reader.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - startOffset));
            }
            this.currentPosition = startOffset;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
        {
        }

        @Override
        public void close() throws IOException
        {
        }

        @Override
        public float getProgress() throws IOException
        {
            if (startOffset == end)
            {
                return 0.0f;
            }
            else
            {
                return Math.min(1.0f, (currentPosition - startOffset) / (float) (end - startOffset));
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException
        {
            if (key == null)
            {
                key = new MultiValuesWordOffset();
                key.setFileName(path.getName());
            }
            key.setOffset(currentPosition);
            if (value == null)
            {
                value = new Text();
            }
            int newSize = 0;
            if (currentPosition < end)
            {
                newSize = reader.readLine(value);
                currentPosition = currentPosition + newSize - 1; // assume that "\r\n" (new line) is one position
                key.setLine(currentLine++);
            }
            if (newSize == 0)
            {
                key = null;
                value = null;
                return false;
            }
            else
            {
                return true;
            }
        }

        @Override
        public MultiValuesWordOffset getCurrentKey() throws IOException, InterruptedException
        {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException
        {
            return value;
        }
    }
}
