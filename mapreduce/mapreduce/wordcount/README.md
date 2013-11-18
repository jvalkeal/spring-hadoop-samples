Spring Hadoop Mapreduce Wordcount Example
=========================================

This example demonstrates the use of Spring Hadoop functionality to
launch a simple mapreduce job.

To run this example, open a command window, go to the the spring-hadoop-examples root directory, and type:

		./gradlew -q run-mapreduce-examples-wordcount

To run this example with non default distro(cdh4yarn cdh4mr1 cdh5yarn cdh5mr1 phd1 hdp13 hdp20 hadoop22 hadoop11 hadoop12),
open a command window, go to the the spring-hadoop-examples root directory, and type:

		./gradlew -q -Pdistro=hadoop22 run-mapreduce-examples-wordcount

To run this example with non default addresses, open a command window, go to the the spring-hadoop-examples root directory, and type:

		./gradlew -q run-mapreduce-examples-wordcount -Dhd.fs=hdfs://localhost:8020 -Dhd.rm=localhost:8032 -Dhd.jt=localhost:9001

To build and test the example

		./gradlew clean :mapreduce-examples-common:mapreduce-examples-wordcount:build

To build without testing

		./gradlew clean :mapreduce-examples-common:mapreduce-examples-wordcount:build -x test


Or to run from your IDE, run one of the following commands once.

		./gradlew eclipse
		./gradlew idea

# Details

In this sample we will execute HDFS operations and a MapReduce job. The MapReduce job is
the familiar wordcount job. The HDFS operations are to first copy a data files into HDFS
and then to remove any existing files in the MapReduce's output directory.

## Sample highlights

The file *src/main/resources/application-context.xml* is the main configuration
file for the sample. It uses the Spring Hadoop XML namespace.

To configure the application to connect to the namenode and jobtracker, use
the *<configuration/>* namespace element. Values in the XML configuration can be
replaced using standard Spring container functionality such as the property placeholder

## Configuring Hadoop connectivity

```xml
<context:property-placeholder location="hadoop.properties"/>

<configuration>
  fs.defaultFS=${hd.fs}
  yarn.resourcemanager.address=${hd.rm}
  fs.default.name=${hd.fs}
  mapred.job.tracker=${hd.jt}
</configuration>
```

To declaratively configure a Java based MapReduce jobs, use the XML namespace *<job/>* element.

## Declaring a Job

```xml
<job id="wordcountJob"
  input-path="${wordcount.input.path}"
  output-path="${wordcount.output.path}"
  libs="file:${app.repo}/hadoop-examples-*.jar"
  mapper="org.apache.hadoop.examples.WordCount.TokenizerMapper"
  reducer="org.apache.hadoop.examples.WordCount.IntSumReducer"/>
```

HDFS scripting is performed using the XML namespace *<script/>* element.

## Declaring a parameterized HDFS script

```xml
<script id="setupScript" location="copy-files.groovy">
  <property name="localSourceFile" value="${app.home}/${localSourceFile}"/>
  <property name="inputDir" value="${wordcount.input.path}"/>
  <property name="outputDir" value="${wordcount.output.path}"/>
</script>
```

The Groovy script that creates output directory, copies local file to
HDFS and removes output HDFS directory is shown below

```
// use the shell (made available under variable fsh)

if (!fsh.test(inputDir)) {
  fsh.mkdir(inputDir);
  fsh.copyFromLocal(localSourceFile, inputDir);
  fsh.chmod(700, inputDir)
}
if (fsh.test(outputDir)) {
  fsh.rmr(outputDir)
}
```

Now that the script and job are managed by the container, they can be dependency
injected into other Spring managed beans. Use the XML namespace *<job-runner/>* element
to configure the list of jobs and HDFS operations actions to execute.
The job-runner element has *pre-action*, *job-ref*, and *post-action* tags
that take a comma delimited list of references to scripts and jobs.

## Declaring a JobRunner

```xml
<job-runner id="runner" run-at-startup="true"
pre-action="setupScript"
job-ref="wordcountJob" />
```

