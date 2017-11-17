package com.hazelcast.Scala

sealed trait UpsertResult
final case object WasInserted extends UpsertResult
final case object WasUpdated extends UpsertResult
