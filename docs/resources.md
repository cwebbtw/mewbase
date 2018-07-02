
# Running Mewbase

## System Requirements

In order to run a mewbase system locally, the following system requirements need to be fulfilled:
* [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) installation or higher
* [Maven 2](https://maven.apache.org/install.html) installation or higher
* [Docker install](https://docs.docker.com/install/) (required for some demos)

## Running Mewbase locally

There are, typically, two ways to get a local installation of mewbase on a a local machine.

### Maven install

The current artefacts for mewbase are available on the repository at [sbtage.com](http://sbtage.com/maven2/). Adding the following entries to a project pom.xml will add the necessary mewbase library to your project:

```xml
<project>

	<repositories>
		<repository>
		    <id>sbtage</id>
		    <name>sbtage staging repo</name>
		    <url>http://sbtage.com/maven2/</url>
		</repository>
	</repositories>

	<dependencies>

	    <dependency>
	      <groupId>io.mewbase</groupId>
	      <artifactId>mewbase-core</artifactId>
	      <version>0.6.0</version>
	    </dependency>

	</dependencies>

</project>
```

### Clone project

Cloning the [github project](https://github.com/tesco/mewbase) will pull all the necessary source code onto your local machine. Note, the project itself is built unsing sbt 1.x, so will require a working [sbt installation](https://www.scala-sbt.org/download.html).

Next, let's look at a simple [Event Sourcing example](https://github.com/Tesco/mewbase/blob/master/docs/commandrest.md) which is exposed over a RESTful interface.
