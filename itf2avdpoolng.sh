#!/bin/bash
export JAVA_HOME=/usr/java/jre1.6.0_03/
echo "Java Home is $JAVA_HOME"

export CLASSPATH=:/home/bjsvwzie/Apps/itf2avdpoolng/itf2avdpoolng.jar:$CLASSPATH
echo "CLASSPATH is $CLASSPATH"

$JAVA_HOME/bin/java -Xms128m -Xmx2048m org.catais.App /home/bjsvwzie/Apps/itf2avdpoolng/itf2avdpoolng.properties


