# mewbase
[![Build Status](https://travis-ci.org/Tesco/mewbase.svg)](https://travis-ci.org/Tesco/mewbase)

## The problem

Mewbase is an eventing toolkit of the JVM. Events are one of the fundamental building blocks when developing a [Reactive System](https://www.reactivemanifesto.org/). Designing and building a system based on events typically involves decomposing the system using techniques such as [event storming](https://www.youtube.com/watch?v=sPseA_3UOMk), and there a number of patterns and techniques in which a system can slowly adapt to be more event driven. The goal of mewbase is to provide a number of general purpose patterns and implementations to make both building, migrating and moving to and between event driven systems simple, and without being prescriptive around underlying infrastructure requirements.

Such examples of patterns used when moving to an event based architecture include:

* Event emitting of changes in a system (typically backed by underlying messaging infrastructure - though mewbase tries to standardise access APIs across providers).
* Subscribing and responding events.
* Separating commands and queries (i.e. CQRS).

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

Many applications typically want to deal with *aggregated* event data, not the raw event data. Examples would
include:

* A website wants to see the *current* state of a shopping basket, not deal with the raw add/remove item events
* A report needs to display average temperature from IoT sensors broken down by region in real-time
* A stock inventory needs to maintain current stock of products per warehouse
* A business process/saga needs to listen to events and maintain state

### Getting started 

[This introduction](https://github.com/Tesco/mewbase/blob/master/docs/introduction.md) provides a solid foundation of main components of a Mewbase system.

To get straight into the code examples, check out: https://github.com/Tesco/mewbase/tree/master/examples-java/src/main/java/example

### Features

Some of the features / future features of mewbase:

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

## Get involved!

This project has a lot of scope and there's a lot to do with some serious engineering challenges.

We have a [Gitter channel](https://gitter.im/mewbase) for general chat. Come and say hi!





