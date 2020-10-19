package com.sme.hadoop.mapreduce.join;

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
 * Unit test of {@link JoinJob}.
 */
class JoinJobTest
{
    private static final String DEPARTMENT = "101,Sales" + NEW_LINE
        + "102,Research" + NEW_LINE
        + "103,Admin" + NEW_LINE;

    private static final String EMPLOYEE = "1,101,James" + NEW_LINE
        + "2,101,Robert" + NEW_LINE
        + "3,102,John" + NEW_LINE
        + "4,103,Oscar" + NEW_LINE
        + "5,101,Olga" + NEW_LINE
        + "6,102,Michael";

    private Path tempDir;
    private Path departmentTempFile;
    private Path employeeTempFile;

    @BeforeEach
    void setUp() throws Exception
    {
        tempDir = Files.createTempDirectory(Paths.get("target"), "hadoopTemp");
        departmentTempFile = Files.createTempFile(tempDir, "Department", ".txt");
        employeeTempFile = Files.createTempFile(tempDir, "Employee", ".txt");

        Files.write(departmentTempFile, DEPARTMENT.getBytes(StandardCharsets.UTF_8));
        Files.write(employeeTempFile, EMPLOYEE.getBytes(StandardCharsets.UTF_8));
    }

    void tearDown() throws Exception
    {
        Files.delete(tempDir);
    }

    @Test
    void testJoinJob() throws Exception
    {
        String output = getOutput();

        String[] args = new String[] {departmentTempFile.toAbsolutePath().toString(), employeeTempFile.toAbsolutePath().toString(), output, "false", "Join", ".json"};

        int exitCode = ToolRunner.run(new JoinJob(), args);
        assertEquals(0, exitCode);

        String text = Files.readAllLines(Paths.get(output + File.separator + "Join.json"), StandardCharsets.UTF_8).stream().collect(Collectors.joining());
        assertEquals("{\"101\":\"Sales:Olga,Robert,James,\",\"102\":\"Research:Michael,John,\",\"103\":\"Admin:Oscar,\"}", text);
    }

    private String getOutput()
    {
        return tempDir.toAbsolutePath().toString() + File.separator + "output";
    }
}
