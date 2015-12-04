[![Build Status](https://drone.io/github.com/nilskp/hazelcast-scala/status.png)](https://drone.io/github.com/nilskp/hazelcast-scala)
[![Scala version](https://img.shields.io/badge/scala-2.11-orange.svg)](http://www.scala-lang.org/api/2.11.7/)
[![Download](https://api.bintray.com/packages/nilskp/maven/hazelcast-scala/images/download.svg)](https://bintray.com/nilskp/maven/hazelcast-scala/_latestVersion#files)
[![Join Chat at https://gitter.im/nilskp/hazelcast-scala](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/nilskp/hazelcast-scala)

## Installation ##
Add this to your project's `build.sbt`:

    resolvers += "nilskp/maven on bintray" at "http://dl.bintray.com/nilskp/maven"

    libraryDependencies += "com.hazelcast" %% "hazelcast-scala" % "latest-integration" withSources()

Hazelcast Scala API
===================

Quick start:

    import com.hazelcast.config._
    import com.hazelcast.Scala._
    
    val conf = new Config
    val hz = conf.newInstance()


## Sample Code ##
See the unit tests for examples of how to use this library.

More to come...
