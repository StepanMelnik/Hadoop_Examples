package com.sme.hadoop.mapreduce.multivalues;

import static com.sme.hadoop.util.Constant.NEW_LINE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test of {@link WordCountJob}.
 */
class MultiValuesJobTest
{
    private static final String TEXT = "James and John in the first line" + NEW_LINE     // 32 + 1 chars
        + "John, Robert and Michael in the second line" + NEW_LINE                       // 43 + 1 chars
        + "James in the third line" + NEW_LINE                                           // 23 + 1 chars
        + "James, Oscar, Olga and Robert in the fourth line" + NEW_LINE                  // 48 + 1 chars
        + " and John,; Robert in the fourth line" + NEW_LINE;                            // 37 + 1 chars

    private Path tempDir;
    private Path tempFile;

    @BeforeEach
    void setUp() throws Exception
    {
        tempDir = Files.createTempDirectory(Paths.get("target"), "hadoopTemp");
        tempFile = Files.createTempFile(tempDir, "MultiValues", ".txt");

        Files.write(tempFile, TEXT.getBytes(StandardCharsets.UTF_8));
    }

    void tearDown() throws Exception
    {
        Files.delete(tempDir);
    }

    @Test
    void testMultiValuesJob() throws Exception
    {
        String output = getOutput();

        String[] args = new String[] {tempFile.toAbsolutePath().toString(), output, "false", "MultiValues", ".json"};

        int exitCode = ToolRunner.run(new MultiValuesJob(), args);
        assertEquals(0, exitCode);

        String result = Files.readAllLines(Paths.get(output + File.separator + "MultiValues.json"), StandardCharsets.UTF_8).stream().collect(Collectors.joining());

        String expectedResult = "{\"James\":\"[{\\\"lineOffset\\\":4,\\\"charOffset\\\":101},{\\\"lineOffset\\\":3,\\\"charOffset\\\":77},{\\\"lineOffset\\\":1,\\\"charOffset\\\":0}]\","
            + "\"John\":\"[{\\\"lineOffset\\\":5,\\\"charOffset\\\":155},{\\\"lineOffset\\\":2,\\\"charOffset\\\":33},{\\\"lineOffset\\\":1,\\\"charOffset\\\":10}]\","
            + "\"Michael\":\"[{\\\"lineOffset\\\":2,\\\"charOffset\\\":50}]\","
            + "\"Robert\":\"[{\\\"lineOffset\\\":5,\\\"charOffset\\\":162},{\\\"lineOffset\\\":4,\\\"charOffset\\\":124},{\\\"lineOffset\\\":2,\\\"charOffset\\\":39}]\"}";

        assertEquals(expectedResult, result);
    }

    private String getOutput()
    {
        return tempDir.toAbsolutePath().toString() + File.separator + "output";
    }
}
