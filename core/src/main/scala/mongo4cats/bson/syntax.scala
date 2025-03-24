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

package mongo4cats.bson

import org.bson.BsonValue
import cats.syntax.all._

trait CodecOps {
  implicit final class BsonEncoderOps[A](val value: A) {
    def asBson(implicit encoder: BsonEncoder[A]): BsonValue = encoder(value)
  }

  implicit final class BsonDocumentEncoderOps[A](val value: A) {
    def asBsonDoc(implicit encoder: BsonDocumentEncoder[A]): BsonDocument = encoder(value)
  }

  implicit class BsonDecoderOps(val value: BsonValue) {
    def as[A: BsonDecoder]: Either[BsonDecodeError, A] = BsonDecoder[A].apply(value)
  }

  implicit class BsonDocumentDecoderOps(val doc: BsonDocument) {
    def as[A: BsonDocumentDecoder]: Either[BsonDecodeError, A] =
      BsonDocumentDecoder[A].apply(doc)

    def getAs[A: BsonDecoder](k: String): Either[BsonDecodeError, A] =
      if (doc.containsKey(k))
        BsonDecoder[A]
          .apply(doc.get(k))
          .leftMap(err => BsonDecodeError(s"At key '$k': ${err.msg}"))
      else BsonDecodeError(s"No key '$k' found").asLeft

    def getOptional[A: BsonDecoder](k: String): Either[BsonDecodeError, Option[A]] =
      if (doc.containsKey(k))
        BsonDecoder[A]
          .apply(doc.get(k))
          .bimap(err => BsonDecodeError(s"At key '$k': ${err.msg}"), Some(_))
      else Right(None)
  }

}

object syntax extends CodecOps
