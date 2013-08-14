Some examples of Storm topologies
====

```
$ sudo apt-get install maven2

$ git clone git@github.com:baijian/storm-java.git

$ cd storm-java

$ mvn package

$ mvn exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.baijian.storm.simple.HelloWorldTopology

$ storm jar /path/to/jar com.baijian.storm.simple.HelloWorldTopology HelloWorld

$ storm kill HelloWorld
```
