# Sample Apache Storm topology

This repo contains simple maven artifact that demonstrates how to prepare
Apache Storm application for execution in cluster.


## Preparing topology jar

In order to bae able to execute topology, we need to prepare jar file with all
dependencies (we need to leave out the storm jars, since they will be added at
submission time). This can easily be done by running `mvn package` command.
This will create `target/wikistats-topology-0.1.0-SNAPSHOT.jar` that can be
now submitted to Storm Nimbus.
