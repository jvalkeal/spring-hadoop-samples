Spring Hadoop Mapreduce Wordcount Example
=========================================

To run this example, open a command window, go to the the spring-yarn-examples root directory, and type:

		./gradlew -q run-mapreduce-examples-wordcount
		
To build and test the example

		./gradlew clean :mapreduce-examples-common:mapreduce-examples-wordcount:build
		
To build without testing

		./gradlew clean :mapreduce-examples-common:mapreduce-examples-wordcount:build -x test


Or to run from your IDE, run one of the following commands once.

		./gradlew eclipse
		./gradlew idea 

