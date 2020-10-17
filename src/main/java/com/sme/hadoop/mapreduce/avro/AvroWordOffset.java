package com.sme.hadoop.mapreduce.avro;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Word Offset POJO.
 */
class AvroWordOffset implements WritableComparable<AvroWordOffset>
{
    private long offset;
    private long line;
    private String fileName;

    public long getOffset()
    {
        return offset;
    }

    public void setOffset(long offset)
    {
        this.offset = offset;
    }

    public long getLine()
    {
        return line;
    }

    public void setLine(long line)
    {
        this.line = line;
    }

    public String getFileName()
    {
        return fileName;
    }

    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.offset = in.readLong();
        this.fileName = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeLong(offset);
        Text.writeString(out, fileName);
    }

    @Override
    public int compareTo(AvroWordOffset multiValuesWordOffset)
    {
        AvroWordOffset that = multiValuesWordOffset;

        int compared = this.fileName.compareTo(that.fileName);
        if (compared == 0)
        {
            return (int) Math.signum((double) (this.offset - that.offset));
        }
        return compared;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof AvroWordOffset)
        {
            return this.compareTo((AvroWordOffset) obj) == 0;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        // CSOFF
        return 42; // not designed
        // CSON
    }
}
