[![Build Status](https://drone.io/github.com/hazelcast/hazelcast-scala/status.png)](https://drone.io/github.com/hazelcast/hazelcast-scala)
[![Scala version](https://img.shields.io/badge/scala-2.11-orange.svg)](http://www.scala-lang.org/api/2.11.8/)
[![Join Chat at https://gitter.im/hazelcast/hazelcast-scala](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/hazelcast/hazelcast-scala)
[![Download](https://api.bintray.com/packages/hazelcast/maven/hazelcast-scala/images/download.svg)](https://bintray.com/hazelcast/maven/hazelcast-scala/_latestVersion#files)

# Installation

The hazelcast-scala API is based on Scala 2.11 and Hazelcast 3.6/3.7, but does not define them as hard dependencies (since it works with both open-source and enterprise Hazelcast), so make sure to also include the relevant Hazelcast dependencies.

## Gradle
Add this to your `build.gradle` file:

    repositories {
      jcenter()
    }
    dependencies {
      compile "org.scala-lang:scala-reflect:2.11.+"
      compile "com.hazelcast:hazelcast:3.7.+" // Or :hazelcast-enterprise:
      compile "com.hazelcast:hazelcast-scala_2.11:3.7.+"
    }

## SBT
Add this to your project's `build.sbt`:

    resolvers += Resolver.jcenterRepo

    libraryDependencies += "com.hazelcast" %% "hazelcast-scala" % "latest-integration" withSources()


# Quick start:

    import com.hazelcast.config._
    import com.hazelcast.Scala._
    
    val conf = new Config
    serialization.Defaults.register(conf.getSerializationConfig)
    val hz = conf.newInstance()


## Sample Code ##
See the [Wiki](../../wiki) and unit tests for examples of how to use this library.
