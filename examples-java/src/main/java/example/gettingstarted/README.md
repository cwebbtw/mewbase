Pre-requisites
==============

Start Kafka and Postgres from docker compose file
-------------------------------------------------

After installing [Docker Compose](https://docs.docker.com/compose/install/), run the follow from the root of mewbase:

```bash
$ docker-compose up
```

This will bring up containers for:
* Kafka :- the event source/sink used in the example
* Postgres :- the binder store used in this example
    * The instance will have mewbase's Postgres schema pre-seeded

Input REST API
==============

Start the input REST api
------------------------

This project will expose mewbase commands over a REST API.

The sample application emits an event to the Kafka event sink
when an external system `POST`s to a command endpoint.

```bash
$ sbt "project examples-java" "runMain example.gettingstarted.commandrest.Main"
```

Hit the input REST api
----------------------

The sample application exposes 2 commands: buy and refund

```bash
$ echo '{"product": "banana", "quantity": 5}' | curl --verbose -X POST -H 'Content-Type:application/json' -d @- http://localhost:9000/buy
```

```bash
$ echo '{"product": "banana", "quantity": 5}' | curl --verbose -X POST -H 'Content-Type:application/json' -d @- http://localhost:9000/refund
```

Projection
==========

Start the projection
--------------------

This project reacts to events received from the Kafka event source.

The sample application maintains a binder with daily purchases/refunds of each product, based events processed.

```bash
$ sbt "project examples-java" "runMain example.gettingstarted.projectionpostgres.Main"
```

Output REST API
===============

Start the output REST API
-------------------------

```bash
$ sbt "project examples-java" "runMain example.gettingstarted.projectionrest.Main"
```


Debugging
=========

Postgres
--------

You can inspect the Postgres schema the sample uses by opening up a `psql` session:

```bash
$ docker exec -t -i mewbase-gs-postgres /usr/bin/psql -U mewbase
```

Kafka
-----

You can view events sent to Kafka topics using `kafka-simple-consumer-shell.sh`:

```bash
$ docker exec -t -i mewbase-gs-kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-simple-consumer-shell.sh --topic purchase_events --broker-list localhost:9092
```