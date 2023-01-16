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

package mongo4cats

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.generic.auto._
import mongo4cats.circe.implicits._
import mongo4cats.circe.unsafe
import mongo4cats.client.MongoClient
import mongo4cats.embedded.EmbeddedMongo
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import io.circe.Json
import io.circe.Encoder

import scala.concurrent.Future
import cats.effect.std.Random
import io.circe.syntax._
import cats.Applicative
import com.mongodb.client.model.changestream.FullDocument
import mongo4cats.bson.BsonDocument
import fs2.Stream
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import org.bson.BsonInt32
import mongo4cats.database.MongoDatabase
import mongo4cats.collection.operations.Filter
import mongo4cats.collection.operations.Update
import fs2.io.file.Path
import mongo4cats.collection.MongoCollection
import mongo4cats.bson.BsonDecoder
import org.bson.BsonTimestamp

class WatchStreamSpec extends AsyncWordSpec with Matchers with EmbeddedMongo {

  import WatchStreamSpec._

  implicit val jsonEnc = unsafe.circeDocumentEncoder[Json]

  override val mongoPort: Int = 12348

  "A WatchStream" should {
    "ignore documents too large" in {
      withEmbeddedMongoClient { client =>
        val result = for {

          db <- client.getDatabase("test")
          _ <- db.createCollection("large")
          coll <- db.getCollection("large")

          watch = recoverWatch[BsonDocument](coll)

          _ <- watch
            .concurrently(
              Stream
                .eval(for {
                  large1 <- largeObject("elems", 10 * 1000 * 1000)

                  _ <- Stream(large1.noSpaces)
                    .through(fs2.text.utf8.encode)
                    .through(
                      fs2.io.file.Files[IO].writeAll(Path("/Users/juanpablosantos/tmp/doc10"))
                    )
                    .compile
                    .drain

                  _ <- IO.println("Going to insert large document")

                  insertedId <- coll.insertOne[Json](large1).map(_.getInsertedId())

                  _ <- IO.println("Inserted large doc")

                  large2 <- largeObject("content", 1 * 1000 * 1000)

                  _ <- coll
                    .updateOne(Filter.eq("_id", insertedId), Update.set("elems2", large2))
                    .attempt

                  _ <- IO.sleep(Duration(5, TimeUnit.SECONDS))

                  _ <- IO.println("Tried to update large document")

                  smol1 <- largeObject("content", 100)

                  c1 <- coll.insertOne[Json](smol1).map(_.getInsertedId()).attempt

                  _ <- IO.println(s"Inserted small document: $c1")

                  smol2 <- largeObject("content", 100)

                  c2 <- coll.insertOne[Json](smol2).map(_.getInsertedId()).attempt

                  _ <- IO.println(s"Inserted small document: $c2")

                } yield ())
            )
            .evalMap { doc =>
              IO.println(s"Got update") >> IO.println(s"Size: ${doc.size}")
            }
            .take(3)
            .compile
            .drain

        } yield ()

        result.map(_ => 1 mustBe 1)
      }
    }
  }

  def recoverWatch[A: BsonDecoder](coll: MongoCollection[IO]): Stream[IO, A] =
    coll.watch
      .fullDocument(FullDocument.UPDATE_LOOKUP)
      .updateStream[A]
      .attempt
      .flatMap {
        case Left(_: com.mongodb.MongoQueryException) =>
          Stream.force(
            for {
              _ <- IO.println("Error, recovering")
              now <- IO.realTimeInstant
              _ = new BsonTimestamp(now.getEpochSecond())
            } yield coll.watch
              .fullDocument(FullDocument.UPDATE_LOOKUP)
              .updateStream[A]
          )
        case Left(t) =>
          Stream.exec(IO.println(s"Another error: $t")) ++ Stream.raiseError[IO](t)
        case Right(e) =>
          Stream.emit(e)
      }

  def withEmbeddedMongoClient[A](test: MongoClient[IO] => IO[A]): Future[A] =
    withRunningEmbeddedMongo {
      MongoClient
        .fromConnectionString[IO](s"mongodb://localhost:$mongoPort")
        .use(client =>
          for {
            admin <- client.getDatabase("admin")
            _ <- admin.runCommand(BsonDocument("replSetInitiate", BsonDocument()))

            _ <- waitUntilReady(client)

            a <- test(client)
          } yield a
        )
    }.unsafeToFuture()

  def waitUntilReady(client: MongoClient[IO]): IO[Unit] =
    (for {
      admin <- client.getDatabase("admin")
      _ <- Stream.repeatEval(serverStatus(admin)).takeWhile(!_).compile.drain
    } yield ()).timeout(Duration(5, TimeUnit.SECONDS))

  def serverStatus(admin: MongoDatabase[IO]): IO[Boolean] =
    for {
      isMaster0 <- admin.runCommand(BsonDocument("isMaster", new BsonInt32(1)))
      isMaster = isMaster0.getBoolean("ismaster")
    } yield isMaster.getValue()

}

object WatchStreamSpec {
  implicit val circleReflEncoder: Encoder[Json] =
    Encoder.instance(j => j)

  def randomAlphaNum(length: Int): IO[String] =
    Applicative[IO]
      .replicateA(
        length,
        Random
          .scalaUtilRandom[IO]
          .flatMap(_.nextAlphaNumeric)
      )
      .map(_.mkString)

  // length in bytes
  def largeObject(key: String, length: Long): IO[Json] = {
    val StringLength = 30
    val charCount = length / StringLength

    val elems =
      Applicative[IO].replicateA(charCount.toInt, randomAlphaNum(StringLength))

    elems.map(es => Json.obj(key := es))
  }
}
