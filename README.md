### Source Code for Kafka Streams in Action

#### Chapter 8 updates

1. Kafka and Kafka Streams are now at version 1.0!
2. With the official release of 1.0, I've been able to remove the kafka-streams and kafka-clients jar files from the project and the related test jar
files as well.
3. Since this chapter is all about testing, all examples run as tests from within your IDE, although if you want you can run tests from the command line
via a `./gradlew clean test` command.
4. All unit tests use JUnit 5.0, so it's good opportunity to learn the changes that have come with the new version.
5. As before, chapter 6 forward uses the new Kafka Streams 1.0 API, chapters 3-5 will get updated before publication to the new API.
may still be some use of deprecated classes and methods here and there.
6. Logging as been added to the application and there is now a logs directory with logs for the application and various examples. Eventually the all code in the project
will get migrated to logging vs sysout.


#### Previous Updates

Here are some notes regarding the source code:

1. Chapter 7 examples are concerned with observing performance so at least one of the examples will continue to run until you explicitly stop the program
from running.  Most of the related results are found in log files or viewed JMX.

2. For the examples in Chapters 5 and 6 since they rely more on timestamps and potential joins, sometimes it takes a few seconds for
data to show up.  Additionally random data is generated for each run of an example
so example runs produce better data than others.  Please be patient.

3. If at first you don't get any results, re-run the example.  It could be a I missed adding a topic name to the create-topics.sh script and the topic does not
exist yet, but Kafka is configured to create topics automatically.


#### Requirements
This project assumes and requires the following

1. Java 8
2. Gradle

If you don't have gradle installed, that's ok, this project uses the gradle wrapper.  This means
the first time you run the ./gradlew or gradlew command gradle will be installed for you.

#### Included Dependencies

1. kafka_2.12-1.0.0.tgz


Kafka itself (version 2.12-1.0.0) is included as a convenience. 

All other dependencies are taken care of via gradle.
 
#### IDE setup
The gradle eclipse and intellij plugins are included in the build.gradle file.
 
1. To set up for eclipse run  ./gradlew eclipse (for windows gradlew eclipse) from the base directory of this repo.
2. To set up for intellij run ./gradlew idea (for windows gradlew idea) from the base directory of this repo.

#### Installing the included Kafka
Run tar xvzf  kafka_2.12-1.0.0.tgz some where on your computer.

#### Running Kafka
1. To start kafka go to <install dir>/kafka_2.12-1.0.0/bin
2. Run zookeeper-server-start.sh
3. Run kafka-server-start.sh

If you are on windows, go to the <install dir>/kafka_2.12-1.0.0/bin/windows directory
and run the .bat files with the same name and in the same order.
 
#### Stopping Kafka
1. To start kafka go to <install dir>/kafka_2.12-1.0.0/bin
2. Run kafka-server-stop.sh
3. Run zookeeper-server-stop.sh

If you are on windows, go to the <install dir>/kafka_2.12-1.0.0/bin/windows directory
and run the .bat files with the same name and in the same order.

#### Sample Kafka Streams Code
All the code from the book can be found in the directory corresponding to the chapter where
the book introduced or demonstrated the concept/code.  Code that is not in a directory named "chapter_N" is either
common code used across all chapters, or utility code.
 
#### Running the Kafka Streams examples
 
All of the example programs can be run from within an IDE or from the command line.  There are gradle
tasks for each of the examples we have so far.  The provided Kafka will need to be running before
you can start any of the examples.  Also there is a script in the bin directory (create-topics.sh) that creates all topics
required (I think I've added all topics, but may have missed one or two).  If you don't run the script that's fine, Kafka auto-creates topics by default.  For the purposes
of our examples that is fine.

All examples should print to the console by default.  Some may write out to topics and print to standard-out
but if you don't see anything in the console you should check the source code to make sure
I did'nt miss adding a print statement.

To run any of the example programs, I recommend running them through the set gradle tasks.  Remember if you are
windows use gradlew instead  ./gradlew to run the program.  All the 
example programs are located in the build.gradle file.  For your convenience here are the commands to run sample programs
we have so far:

1. ./gradlew runYellingApp (Kafka Streams version of Hello World)
2. ./gradlew runZmartFirstAppChapter_3
3. ./gradlew runZmartAdvancedChapter_3
4. ./gradlew runAddStateAppChapter_4
5. ./gradlew runJoinsExampleAppChapter_4
6. ./gradlew runAggregationsChapter_5
7. ./gradlew runCountingWindowingChapter_5
8. ./gradlew runGlobalKtableChapter_5
9. ./gradlew runKStreamKTableChapter_5
10. ./gradlew runPopsHopsChapter_6
11. ./gradlew runStockPerformance_Chapter_6
12. ./gradlew runStockPerformanceStreamsProcess_Chapter_6
13. ./gradlew runCoGrouping_Chapter_6
14. ./gradlew runCoGroupinStateRetoreListener_Chapter_7
15. ./gradlew runStockPerformanceConsumerInterceptor_Chapter_7
16. ./gradlew runZmartJmxAndProducerInterecptor_Chapter_7

#### Example Kafka Streams Program Output
When running the examples, the program will generate data to flow through Kafka and into the sample
streams program.  The data generation occurs in the background.  The Kafka Streams programs will run for 
approximately one minute each.  The sample programs write results to the console as well as topics.  While you
are free to use the ConsoleConsumer or your own Consumer, it's much easier to view the results flowing to the console.