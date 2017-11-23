# mewbase
[![Build Status](https://travis-ci.org/Tesco/mewbase.svg)](https://travis-ci.org/Tesco/mewbase)

## The problem

Many organisations now realise that it's vital to keep track of all the streams of events they produce.

Examples of streams of events might be:

* Shopping basket events (add/remove/order) from a website
* Events from IoT sensors
* Stock movement events in a supply chain

These events often need to be persisted fast and in high volume and potentially stored for very long periods of time. 

Storing logs of events provides many advantages including:

* Complete audit trail of everything that happened
* Events can be replayed for back-testing or debugging
* Analytics can be run on the events to extract business value
* Machine learning models can be trained in the events to do all sorts of funky stuff
* Events can be used to co-ordinate busines processes between different services
* Functions can be used to implement distributed eventually consistent transactions

But many applications typically want to deal with *aggregated* event data, not the raw event data. Examples would
include:

* A website wants to see the *current* state of a shopping basket, not deal with the raw add/remove item events
* A report needs to display average temperature from IoT sensors broken down by region in real-time
* A stock inventory needs to maintain current stock of products per warehouse
* A business process/saga needs to listen to events and maintain state

This leads many applications to either:

* Not capture raw events at all - only deal with the current state of their objects - thus losing valuable business
data
* Maintain their own "views" on the event log data perhaps by subscribing to messages and updating tables or documennts
in a separate database

The latter is usually implemented in an ad-hoc way for each application and requires several moving parts, and has to deal
with the synchronization between events and views. What's more it means the client has to deal with accessing two separate
pieces of technology - a messaging system and a database.

## Introducing mewbase

Mewbase provides a library that abstracts a streaming event store, such as Nats Streaming server. 
Allowing you to run persistent *functions* within the library that can listen to raw events and maintain multiple 
*projections* of the event data.
 
The projections can then be queried in similar way to how you would with a document or graph database.

So you can think of mewbase as combining the following:

* An event streaming protocol
* A document/graph database protocol
* Functions which map events to documents

An interesting observation is, unlike a normal document database, documents derived from the mewbase library are not 
updated directly by clients, they are only updated in the engine by the functions.

The client simply sends events (*add item to basket*), and issues queries (*give me current state of basket 1234*)(*see 
shopping examples package within this project). That means the client has no access to shared mutable state - a big win!  

The events, functions and documents are all maintained by the same library, mewbase keeps track of synchronization
between the events and the views so you know they are always up to date.

Since the projection functions run within the library, you can write them in Java - no need to learn another language like SQL. 
The same applies to commands and queries.

The Mewbase library is fully embeddable in any Java (or Scala, or Groovy) application and  is a great match 
for implementing event driven microservices using an event sourcing / CQRS pattern.

Mewbase has adapters for transforming CQRS commands and queries into REST based server APIs allowing servers 
to expose library faciliteis via REST interfaces in (Vert.x and SpringBoot *coming soon*)

### Features

Some of the features / future features:

Library Features

* BSON messages
* JSON to Bson and Bson to JSON transformations
* Projection Creation and Management
* User defined functions
* Metrics
* Completely non blocking, and reactive APIs
* CQRS Toolkit
* Live persistent queries - Document updates to event channels
* Fully embeddable in Java 9, Scala, Groovy, etc servers.

Facilities assumed in protocols for Event Sinks and Sources. 

* Event streaming
* Multiple event logs
* Event persistence
* Event subscription
* Subscription filters

Facilities assumed in protocols for Document Stores

* Persistent documents (Bson as Standard)
* Document queries
* Document streaming


### Why the name "mewbase"?

"mew" is the UK pronunciation of the Greek letter μ. This letter is often used in the scientific world to represent
"micro" i.e. 10^-6 or more loosely "something small". Mewbase is a great choice for event based microservices hence
the connection.

Also, more importantly, "mew" is the sound made by a kitten, and it would be great to have a logo of a cute kitten for
the project. (Any volunteers?)

## Get involved!

This project has a lot of scope and there's a lot to do with some serious engineering challenges.

If you'd like to get involved and be a part of it, please say hi in our
[google group](https://groups.google.com/forum/#!forum/mewbase).

We also have a gitter channel for general chat. Come and say hi!

[Gitter channel](https://gitter.im/mewbase)

## Roadmap

Roadmap and other pages are on the [wiki](https://github.com/Tesco/mewbase/wiki)

## Install Library and Run Examples

Mewbase can be run embedded in any Java application (see examples package).

The current version of Mewbase depends on a running instance of a [nats-streaming](https://nats.io/documentation/streaming/nats-streaming-intro/) server instance.  Assuming you have [docker](https://www.docker.com/) locally installed, the quickest and easiest way to up and running is via following commands:

    docker pull nats-streaming
    docker run -p 4222:4222 -p 8222:8222 -d nats-streaming


Mewbase can then be built and installed into your local maven repository so that 
you can include it as a library dependency into other client and server projects   

    mvn clean install
 
 
Refer to the maven output for the the current library version number which you can add to 
maven pom.xml build files, for example, and the example code can be run from the resulting 
mewbase-<version>.jar file or from your project IDE.

## Generate mewbase project from template

A template Mewbase project can also be generated using the following command:

    mvn archetype:generate 	-DarchetypeGroupId=io.mewbase 	\
    			-DarchetypeArtifactId=mewbase 	\
    			-DarchetypeVersion=0.2.1  	\
    			-DgroupId=[sample.groupId]	\
    			-DartifactId=[mySampleArtifact]	
