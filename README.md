### Source Code for Kafka Streams in Action


#### Requirements
This project assumes and requires the following

1. Java 8
2. Gradle

If you don't have gradle installed, that's ok, this project uses the gradle wrapper.  This means
the first time you run the ./gradlew or gradlew command gradle will be installed for you.

#### Included Dependencies

1. kafka_2.11-0.10.3.0 SNAPSHOT.tgz
2. kafka-streams-0.10.3.0-SNAPSHOT.jar
3. kafka-clients-0.10.3.0-SNAPSHOT.jar

These dependencies are included because I wanted to present some of the latest features and this book
is targeting the 0.10.3.0 release of Kafka for publication.  All other dependencies are taken care of via gradle.
 
#### IDE setup
The gradle eclipse and intellij plugins are included in the build.gradle file.
 
1. To set up for eclipse run  ./gradlew eclipse (for windows gradlew eclipse) from the base directory of this repo.
2. To set up for intellij run ./gradlew idea (for windows gradlew idea) from the base directory of this repo.

#### Installing the included Kafka
Run tar xvzf  kafka_2.11-0.10.3.0 SNAPSHOT.tgz some where on your computer.

#### Running Kafka
1. To start kafka go to <install dir>/kafka_2.11-0.10.3.0-SNAPSHOT/bin
2. Run zookeeper-server-start.sh
3. Run kafka-server-start.sh

If you are on windows, go to the <install dir>/kafka_2.11-0.10.3.0-SNAPSHOT/bin/windows directory
and run the .bat files with the same name and in the same order.
 
#### Stopping Kafka
1. To start kafka go to <install dir>/kafka_2.11-0.10.3.0-SNAPSHOT/bin
2. Run kafka-server-stop.sh
3. Run zookeeper-server-stop.sh

If you are on windows, go to the <install dir>/kafka_2.11-0.10.3.0-SNAPSHOT/bin/windows directory
and run the .bat files with the same name and in the same order.

#### Sample Code
All the code from the book can be found in the directory corresponding to the chapter where
the book introduced or demonstrated the concept/code.  Code that is not in a directory named "chapter_N" is either
common code used across all chapters, or utility code.
 
#### Running the examples
 
All of the example programs can be run from within an IDE or from the command line.  There are gradle
tasks for each of the examples we have so far.  The provided Kafka will need to be running before
you can start any of the examples.  Also there is a script in the bin directory (create-topics.sh) that creates all topics
required.  If you don't run the script that's fine, Kafka auto-creates topics by default.  For the purposes
of our examples that is fine.

To run any of the example programs, I recommend running them through the set gradle tasks.  Remember if you are
windows use gradlew instead  ./gradlew to run the program.  All the 
example programs are located in the build.gradle file.  For your convenience here are the commands to run sample programs
we have so far:

1. ./gradlew runYellingApp (Kafka Streams version of Hello World)
2. ./gradlew runZmartFirstAppChapter_3
3. ./gradlew runZmartAdvancedChapter_3
4. ./gradlew runAddStateAppChapter_4
5. ./gradlew runJoinsExampleAppChapter_4

#### Example Program Output
When running the examples, the program will generate data to flow through Kafka and into the sample
streams program.  The data generation occurs in the background.  The Kafka Streams programs will run for 
approximately one minute each.  The sample programs write results to the console as well as topics.  While you
are free to use the ConsoleConsumer or your own Consumer, it's much easier to view the results flowing to the console.