package com.sme.hadoop.mapreduce.wordcount;

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
class WordCountJobTest
{
    private static final String TEXT = "James and John in the first line" + NEW_LINE    // 32 + 1 chars
        + "John, Robert and Michael in the second line" + NEW_LINE                      // 43 + 1 chars
        + "James in the third line" + NEW_LINE                                          // 23 + 1 chars
        + "James, Oscar, Olga and Robert in the fourth line";                           // 48 + 1 chars

    private Path tempDir;
    private Path tempFile;

    @BeforeEach
    void setUp() throws Exception
    {
        tempDir = Files.createTempDirectory(Paths.get("target"), "hadoopTemp");
        tempFile = Files.createTempFile(tempDir, "WordCount", ".txt");

        Files.write(tempFile, TEXT.getBytes(StandardCharsets.UTF_8));
    }

    void tearDown() throws Exception
    {
        Files.delete(tempDir);
    }

    @Test
    void testWordCountJob() throws Exception
    {
        String output = getOutput();

        String[] args = new String[] {tempFile.toAbsolutePath().toString(), output, "false", "WordCount", ".json"};

        int exitCode = ToolRunner.run(new WordCountJob(), args);
        assertEquals(0, exitCode);

        String text = Files.readAllLines(Paths.get(output + File.separator + "WordCount.json"), StandardCharsets.UTF_8).stream().collect(Collectors.joining());
        assertEquals("{\"James\":3,\"John\":2,\"Michael\":1,\"Olga\":1,\"Oscar\":1,\"Robert\":2,\"and\":3,\"first\":1,\"fourth\":1,\"in\":4,\"line\":4,\"second\":1,\"the\":4,\"third\":1}", text);
    }

    private String getOutput()
    {
        return tempDir.toAbsolutePath().toString() + File.separator + "output";
    }
}
