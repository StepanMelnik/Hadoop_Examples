package com.sme.hadoop.mapreduce.avro;

import org.apache.avro.Schema;

/**
 * Contains constants to work with Mapper/Reducer properties.
 */
public class AvroWordOffsetConstant
{
    public static final String FILE_NAME = "fileName";
    public static final String LINE = "line";
    public static final String OFFSET = "offset";
    public static final String RESULT = "result";

    // CSOFF
    static final Schema AVRO_WORD_OFFSET_SCHEMA = new Schema.Parser().parse(
            "{" +
                "  \"type\": \"record\"," +
                "  \"name\": \"AvroWordOffset\"," +
                "  \"doc\": \"AvroWordOffset bean\"," +
                "  \"fields\": [" +
                "    {\"name\": \"fileName\", \"type\": \"string\"}," +
                "    {\"name\": \"line\", \"type\": \"int\"}," +
                "    {\"name\": \"offset\", \"type\": \"int\"}," +
                "    {\"name\": \"result\", \"type\": \"string\"}" +
                "  ]" +
                "}");
    // CSON

    // private
    private AvroWordOffsetConstant()
    {
    }
}
