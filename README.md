# Sample Java projects for testing DICE deployment

This repository contains projects for demonstrating and experimenting with the
deployment aspects of the Data Intensive Application (DIA) development in Java.
The examples focus around the WikiStats use case. Currently, they support
Apache Storm and Apache Spark.

## IDE

### Importing Java projects

In Eclipse, use **File** -> **Import ...** and pick **Existing Maven projects**.
For the **Root Directory**, select the directory where you cloned or unpacked
the repository to. Eclipse will find the two `pom.xml` files in each of the
subdirectories for Storm and Spark. Run the wizard to the end, and your
Java workspace should contain the projects that you have selected.

### Building the projects

The projects are regular Java projects, which behave in Eclipse like any usual
Java project. But before running a deployment, we need to execute the Maven
goals as well.

To do this, we right-click the project in the **Package Explorer** and select
**Run as** -> **Maven install**. This makes certain that the `.jar` file is
built and copied into the `lib/` directory (which also gets created along the
way).

## Deploying the blueprints

This applies to both the projects. Once you have the blueprint YAML (either
created manually, obtained in the repository or created using DICER),
open the **DICE Tools** -> **DICE Deployments** and create a new (or pick
an existing) Run Configuration. The **Main blueprint file** will be the
`.yaml` file that you wish to deploy, probably located in the `blueprints/`
subdirectory of the project. The **Resource folder** will be the `lib/`
subdirectory of the project.

## Command line

### Preparing Storm topology jar

In order to be able to execute topology, we need to prepare jar file with all
dependencies (we need to leave out the storm jars, since they will be added at
submission time). This can easily be done by running `mvn package` command.
This will create `target/wikistats-topology-0.1.0-SNAPSHOT.jar` that can be
now submitted to Storm Nimbus.
