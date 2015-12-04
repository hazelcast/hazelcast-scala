[![Build Status](https://drone.io/github.com/nilskp/hazelcast-scala/status.png)](https://drone.io/github.com/nilskp/hazelcast-scala)
[![Scala version](https://img.shields.io/badge/scala-2.11-orange.svg)](http://www.scala-lang.org/api/2.11.7/)
[![Join Chat at https://gitter.im/nilskp/hazelcast-scala](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/nilskp/hazelcast-scala)
[![Download](https://api.bintray.com/packages/nilskp/maven/hazelcast-scala/images/download.svg)](https://bintray.com/nilskp/maven/hazelcast-scala/_latestVersion#files)

# Installation

The hazelcast-scala API is based on Scala 2.11 and Hazelcast 3.6, but does not define them as hard dependencies, so make sure to also include the relevant Hazelcast dependencies.

## Gradle
Add this to your `build.gradle` file:

    repositories {
      maven {
        url "http://dl.bintray.com/nilskp/maven" 
      }
    }
    dependencies {
      compile "org.scala-lang:scala-reflect:2.11.+"
      compile "com.hazelcast:hazelcast-all:3.6+"
      compile "com.hazelcast:hazelcast-scala_2.11:1+"
    }

## SBT
Add this to your project's `build.sbt`:

    resolvers += "nilskp/maven on bintray" at "http://dl.bintray.com/nilskp/maven"

    libraryDependencies += "com.hazelcast" %% "hazelcast-scala" % "latest-integration" withSources()


# Quick start:

    import com.hazelcast.config._
    import com.hazelcast.Scala._
    
    val conf = new Config
    serialization.DefaultSerializers.register(conf.getSerializationConfig)
    val hz = conf.newInstance()


## Sample Code ##
See the unit tests for examples of how to use this library.

More to come...
