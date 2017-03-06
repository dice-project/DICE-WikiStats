# Wikistats-Models

Introduction

This document concisely describes the methodological steps to follow in order to create DICE UML models from scratch so that other  DICE-provided tools can be run. In particular, just as a reference example, here we refer to the required modeling procedure to apply if you want to use the DICER tool. For other tools the rest of the methodological description can be found in the rest of the methodology document, particularly in the stand-alone scenarios specific for each tool in the DICE ecosystem.
For the scope of our example, a certain user is willing to use DICER a tool developed in the context of DICE, along with the modeling procedure here presented, which enable, for this particular user, the automatic deployment of a Data Intensive Application (DIA), starting from its DICE UML models and generating TOSCA (Topology and Orchestration Specification for Cloud Applications) compliant deployment blueprints, which can, in turn, be processed by the DICE Deployment Service and deployed successfully. 

In the following sections you will be guided through the creation and definition within the DICE IDE of the various UML model envisioned by the DICE approach, which are the Dice Platform Independent Model (DPIM), the Dice Technology Specific Model (DTSM) and the Dice Deployment Specific Model (DDSM).

DPIM modeling

Our imaginary user starts by creating a new DICE Papyrus UML project and selecting the “Class” and “Deployment” kinds of diagram for our model.
Our user shall then open the created class diagram and instantiate two packages, one for his DPIM model and another for his DTSM model. On these two packages our user has  to apply the DICE::DPIM and the DICE:DTSM UML profiles respectively.


At this point, in the DPIM package our user can model the high level architecture of his DIA, as a class diagram representing the kind of computations over various data sources. In order to model this, our user can instantiate a new class and apply on it the "DiceComputationNode" stereotype. Our user can also model various data sources which can be either profiled using the "DiceSourceNode" of the "DiceStorageNode" stereotypes, depending on the kind of data source. Our user can finally associate his computations to the available data sources.
Let us model a simple DIA called WikiStats, which basically analyze web pages from the popular Wikimedia website  (a class with the <<DpimSourceNode>> stereotype) and stores the result of the analysis into a database  (a class with the "DpimStorageNode" stereotype). So far we don’t add any technological aspect of our application, such as the implementing technologies for the WikistatsApplication and WikistatsStorage nodes or the actual implementation of the WikistatsApplication node.

DTSM modeling

In the DTSM package you can model the actual implementation of the various computations you declared in the DPIM package, plus all the required technology-specific details. You can decide which technology to use for implementing the various components of our user DIA. For instance, let’s suppose you want to use Apache Storm to detail the implementation of the “WikistatsApplication” "DiceComputationNode" in the DPIM model as a Storm topology. You can use the stereotypes from the DICE::DTSM-Storm profile to fully describe our user Storm application. 


Once you have such a DTSM diagram, you might execute some analysis on it, such as formal verification analysis to verify safety properties of our user model.


DDSM modeling

As a last step our user can open the Deployment Diagram of our user UML model, to model the deployment of you DIA. You can start by applying the DICE::DDSM profile on our user Deployment Diagram. The available stereotypes allow you to model the required Cloud resources, such as virtual machines, and the allocation of the various Big Data middlewares required to execute our user application. For instance in the case of a Storm application, you need to instantiate two middlewares, the Apache Zookeeper platform and the Apache Storm platform, which depends on the former. Then you need to specify the required infrastructure for you middlewares. Let’s suppose you want to deploy both Zookeeper and Storm on the same Cluster of VMs, which also means with the same number of replicas. In order to do so you can first instantiate a Device and apply the "DdsmVm" on it. Using the "DdsmVm" stereotype you can specify various properties of our user VMs cluster, such as the number of available VMs, and the Cluster provider. 


You can then put inside our user VMs cluster a first Node and apply the "DdsmZookeeperServer" stereotype. Using the "DdsmZookeeperServer" stereotype you can specify the various properties of our user Zookeeper cluster. You can repeat the same process for a second Node tagged with the "DdsmStormCluster" stereotype and you can model the dependency of Storm on Zookeeper by simply connect the two nodes with a Dependency element. 




Since our Wikistats application will use store the result of its analysis into a database, let’s imagine the user decide to use Apache Cassandra for this purpose. Thus we have to model the deployment of a Cassandra cluster, which will be accessed by our application. Let’ also imagine we want to host the Cassandra cluster on a different VMs cluster from the one hosting Storm and Zookeeper. We then instantiate a new Device and we apply the "DdsmVm" stereotype on it. We can then put a new Node within the just created Device and apply the "DdsmCassandraCluster" stereotype on it. We can finally specify the various properties of our Cassandra cluster using to the applied stereotypes.



Finally our user can model the deployment of his application by instantiating an Artifact on which to apply the BigDataJob stereotype. Our user can specify the required deployment information for his application, such as the location of the application runnable artifact and thusspecify to which of the available Big Data middleware our user DIA has to be submitted, using the DICE Deployment service.



*** DDSM Modelling ***

First of all the user is assumed to have prepared the DDSM diagram, in case you didn’t do this, jump to the appropriate procedure <put link to DDSM creation cheat-sheet from Diego’s cheat-sheet>. 
Following on, a rollout and deployment procedure can ensue exploiting the DICER plugin to generate the associated deployable TOSCA blueprint. In order to do so you right-click on the UML model on the model-navigator pane (usually in the left-hand side of the DICE IDE) and select to run through it a run configuration, specifically pressing the “DICER launcher” run-configuration. 
Pressing the appropriate run-configuration button (“Generate TOSCA Blueprint”) will lead to the run-configuration config: here you can set the URL to contact the DICER service (a local default is always present and pre-configured); you can optionally specify the path to the input model and for the output model to be placed. 
Finally, you can click on the “run” button in the lower-right corner of the run-configuration pop-up window, to generate the TOSCA blueprint; Pressing F5 and refreshing the workspace will reveal the generated blueprint.
The TOSCA blueprint should be submitted that to the DICE Deployment Service for further processing - see the Deployment Service cheat-sheet <put link to deploymentService cheat-sheet>.


