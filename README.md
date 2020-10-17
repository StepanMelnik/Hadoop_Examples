# Hadoop_Examples 3.3

The project contains unit tests to work with Hadoop MapReduce, Avro, Hive and HDFS (TODO Hive and HDFS in process).

## Description

Unit tests prepare temp test data in **/target** folder and starts Hadoop jobs with predefined input parameters.

The tests use JsonOutputFormat writer to assert data in clear format.
 
### Multivalues Hadoop job

Multivalues Hadoop job prepares the same solution as created in <a href="https://github.com/StepanMelnik/LargeFileParser">LargeFileParser</a> project.

**MultiValuesJobTest.java** resolves the same idea as described in **LargeFileParser** project and asserts the same json result.

**AvroWordOffsetJobTest.java** resolves the same idea as described in **LargeFileParser** project and asserts the same avro-to-json result.
