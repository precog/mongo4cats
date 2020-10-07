package mongo4cats.database

import cats.effect.{Async, Concurrent, Sync}
import cats.implicits._
import mongo4cats.database.codecs.MongoCodecRegistry
import mongo4cats.database.helpers._
import org.mongodb.scala.MongoDatabase

import scala.reflect.ClassTag

final class MongoDatabaseF[F[_]: Concurrent] private(
    private val database: MongoDatabase
) {

  def name: F[String] =
    Sync[F].pure(database.name)

  def getCollection[T: ClassTag](name: String)(implicit codec: MongoCodecRegistry[T]): F[MongoCollectionF[F, T]] =
    Sync[F]
      .delay(database.withCodecRegistry(codec.get).getCollection[T](name))
      .flatMap(MongoCollectionF.make[F, T])

  def collectionNames(): F[Iterable[String]] =
    Async[F].async { k =>
      database.listCollectionNames().subscribe(multipleItemsObserver[String](k))
    }

  def createCollection(name: String): F[Unit] =
    Async[F].async { k =>
      database.createCollection(name).subscribe(voidObserver(k))
    }
}

object MongoDatabaseF {
  def make[F[_]: Concurrent](database: MongoDatabase): F[MongoDatabaseF[F]] =
    Sync[F].delay(new MongoDatabaseF[F](database))
}