/*
 * Copyright 2020 Kirill5k
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mongo4cats.collection.operations

import com.mongodb.client.model.{Aggregates, BucketAutoOptions, GraphLookupOptions, MergeOptions, UnwindOptions}
import mongo4cats.bson.Encoder
import mongo4cats.bson.syntax._
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters._

final case class Aggregate private (private val aggs: List[Bson]) {
  def bucketAuto[T: Encoder](groupBy: T, buckets: Int, options: BucketAutoOptions): Aggregate =
    add(Aggregates.bucketAuto(groupBy.asBson, buckets, options))

  def sample(size: Int): Aggregate =
    add(Aggregates.sample(size))

  def count: Aggregate =
    add(Aggregates.count())

  def count(field: String): Aggregate =
    add(Aggregates.count(field))

  def matchBy(filter: Filter): Aggregate =
    add(Aggregates.`match`(filter.toBson))

  def project(projection: Projection): Aggregate =
    add(Aggregates.project(projection.toBson))

  def sort(sort: Sort): Aggregate =
    add(Aggregates.sort(sort.toBson))

  def sortByCount[T: Encoder](filter: T): Aggregate =
    add(Aggregates.sortByCount(filter.asBson))

  def skip(n: Int): Aggregate =
    add(Aggregates.skip(n))

  def limit(n: Int): Aggregate =
    add(Aggregates.limit(n))

  def lookup(from: String, localField: String, foreignField: String, as: String): Aggregate =
    add(Aggregates.lookup(from, localField, foreignField, as))

  def group[T: Encoder](id: T, fieldAccumulator: Accumulator): Aggregate =
    add(Aggregates.group(id.asBson, fieldAccumulator.toBsonFields))

  def unwind(fieldName: String, unwindOptions: UnwindOptions = new UnwindOptions()): Aggregate =
    add(Aggregates.unwind(fieldName, unwindOptions))

  def out(collectionName: String): Aggregate =
    add(Aggregates.out(collectionName))

  def out(databaseName: String, collectionName: String): Aggregate =
    add(Aggregates.out(databaseName, collectionName))

  def merge(collectionName: String, options: MergeOptions = new MergeOptions()): Aggregate =
    add(Aggregates.merge(collectionName, options))

  def replaceWith[T: Encoder](value: T): Aggregate =
    add(Aggregates.replaceWith(value.asBson))

  def lookup(from: String, pipeline: Aggregate, as: String): Aggregate =
    add(Aggregates.lookup(from, pipeline.toBsons, as))

  def graphLookup[T: Encoder](
      from: String,
      startWith: T,
      connectFromField: String,
      connectToField: String,
      as: String,
      options: GraphLookupOptions = new GraphLookupOptions()
  ): Aggregate =
    add(Aggregates.graphLookup(from, startWith.asBson, connectFromField, connectToField, as, options))

  def unionWith(collection: String, pipeline: Aggregate): Aggregate =
    add(Aggregates.unionWith(collection, pipeline.toBsons))

  def combineWith(other: Aggregate) =
    copy(aggs = other.aggs ::: aggs)

  def toBsons: java.util.List[Bson] =
    aggs.reverse.asJava

  private def add(doc: Bson): Aggregate =
    copy(aggs = doc :: aggs)

}

object Aggregate {
  def empty: Aggregate = Aggregate(List.empty)
}
