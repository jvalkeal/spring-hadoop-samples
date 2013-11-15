Spring Yarn with Managed Container Groups Example
=================================================

		# gradlew clean :yarn-examples-common:yarn-examples-container-groups:build

		# gradlew -q run-yarn-examples-container-groups

		# gradlew -q run-yarn-examples-container-groups -Dhd.fs=hdfs://192.168.223.170:8020 -Dhd.rm=192.168.223.170:8032 -Dlocalresources.remote=hdfs://192.168.223.170:8020

		# gradlew -q run-yarn-examples-container-groups -Pgroup=groupname -Psize=1

