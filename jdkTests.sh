#!/bin/bash

JDK7=1
JDK8=0

if [[ $(whoami) == "root" ]]; then
  echo "Don't run this as root."
  exit 1
fi

echo "Switching to Java7..."
sudo update-alternatives --config java <<< $JDK7 1> /dev/null
sudo update-alternatives --config javac <<< $JDK7 1> /dev/null

javac_version=$(javac -version 2>&1 | awk '{print $2}')
java_version=$(java -version 2>&1 | head -1 | awk '{print $3}' | sed 's/"//g')

echo -e "Java compiler version: $javac_version"
echo -e "Java runtime versioin: $java_version \n"

echo "Running tests under JDK7..."
sbt clean update compile test 2>&1 >> jdk7-test.log

tail -1 jdk7-test.log

echo -e "\n\nSwitching to Java8..."

sudo update-alternatives --config java <<< $JDK8 1>  /dev/null
sudo update-alternatives --config javac <<< $JDK8 1> /dev/null

javac_version=$(javac -version 2>&1 | awk '{print $2}')
java_version=$(java -version 2>&1 | head -1 | awk '{print $3}' | sed 's/"//g')

echo -e "Java compiler version: $javac_version"
echo -e "Java runtime versioin: $java_version \n"

echo "Running tests under JDK8..."
sbt clean update compile test 2>&1 >> jdk8-test.log

tail -1 jdk8-test.log

echo "Done..."
