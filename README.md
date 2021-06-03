[![Build Status](https://semaphoreci.com/api/v1/nilskp/hazelcast-scala/branches/master/badge.svg)](https://semaphoreci.com/nilskp/hazelcast-scala)
[![Scala version](https://img.shields.io/badge/scala-2.11-orange.svg)](https://www.scala-lang.org/api/2.11.x/)
[![Scala version](https://img.shields.io/badge/scala-2.12-orange.svg)](https://www.scala-lang.org/api/2.12.x/)
[![Scala version](https://img.shields.io/badge/scala-2.13-orange.svg)](https://www.scala-lang.org/api/2.13.x/)
[![Join Chat at https://gitter.im/hazelcast/hazelcast-scala](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/hazelcast/hazelcast-scala)
[![Download](https://api.bintray.com/packages/hazelcast/maven/hazelcast-scala/images/download.svg)](https://bintray.com/hazelcast/maven/hazelcast-scala/_latestVersion#files)


---
**REMARK**

Dear community members,

Thanks for your interest in **hazelcast-scala**! As of June 2021, this project has become a Hazelcast Community project.

Hazelcast Inc. gives this project to the developers community in the hope you can benefit from it. It comes without any maintenance guarantee by the original developers but their goodwill (and time!). We encourage you to use this project however you see fit, including any type of contribution in the form of a pull request or an issue. 

Feel free to visit our Slack Community for any help and feedback.

---

# Installation

The hazelcast-scala API is based on Scala 2.11/2.12/2.13 and Hazelcast 3.12, but does not define them as hard dependencies (since it works with both open-source and enterprise Hazelcast, and multiple versions), _so make sure to also include the relevant Hazelcast dependencies explicitly_.

## Gradle
Add this to your `build.gradle` file:

```groovy
repositories {
  jcenter()
  mavenCentral()
}

dependencies {
  compile "org.scala-lang:scala-reflect:2.12.+"
  compile "com.hazelcast:hazelcast:3.12.+" // Or :hazelcast-enterprise:
  compile "com.hazelcast:hazelcast-scala_2.12:3.12.+"
}
```

## SBT
Add this to your project's `build.sbt`:

```scala
resolvers += Resolver.jcenterRepo

libraryDependencies += "com.hazelcast" %% "hazelcast-scala" % "latest-integration" withSources()
```

# Quick start:

```scala
import com.hazelcast.config._
import com.hazelcast.Scala._

val conf = new Config
serialization.Defaults.register(conf.getSerializationConfig)
val hz = conf.newInstance()
```

## Sample Code ##
See the [Wiki](../../wiki) and unit tests for examples of how to use this library.
