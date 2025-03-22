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

import org.bson._
import java.time.Instant

final case class BsonDecodeError(msg: String) extends Exception(msg)

trait BsonDecoder[A] extends Serializable { self =>
  def apply(b: BsonValue): Either[BsonDecodeError, A]

  def map[B](f: A => B): BsonDecoder[B] = new BsonDecoder[B] {
    def apply(b: BsonValue): Either[BsonDecodeError, B] =
      self.apply(b).map(f)
  }
}

trait BsonDocumentDecoder[A] extends Serializable { self =>
  def apply(b: BsonDocument): Either[BsonDecodeError, A]

  def map[B](f: A => B): BsonDocumentDecoder[B] = new BsonDocumentDecoder[B] {
    def apply(b: BsonDocument): Either[BsonDecodeError, B] =
      self.apply(b).map(f)
  }
}

object BsonDocumentDecoder extends LowLevelDocumentDecoder {
  def apply[A](implicit ev: BsonDocumentDecoder[A]): BsonDocumentDecoder[A] = ev

  def instance[A](f: BsonDocument => Either[BsonDecodeError, A]) = new BsonDocumentDecoder[A] {
    def apply(b: BsonDocument) = f(b)
  }

  implicit val bsonDocumentDecoder: BsonDocumentDecoder[BsonDocument] =
    instance[BsonDocument](Right(_))
}

trait LowLevelDocumentDecoder {
  implicit def narrowDecoder[A: BsonDecoder]: BsonDocumentDecoder[A] =
    BsonDocumentDecoder.instance[A] { (b: BsonDocument) =>
      BsonDecoder[A].apply(b: BsonValue)
    }
}

object BsonDecoder {
  def apply[A](implicit ev: BsonDecoder[A]): BsonDecoder[A] = ev

  def instance[A](f: BsonValue => Either[BsonDecodeError, A]) = new BsonDecoder[A] {
    def apply(b: BsonValue) = f(b)
  }

  implicit val bsonDecoder: BsonDecoder[BsonValue] =
    instance[BsonValue](Right(_))

  implicit val int: BsonDecoder[Int] = BsonDecoder.instance {
    case x: BsonInt32 => Right(x.getValue)
    case _            => Left(BsonDecodeError("Expected BsonInt32"))
  }

  implicit val long: BsonDecoder[Long] = BsonDecoder.instance {
    case x: BsonInt64 => Right(x.getValue)
    case _            => Left(BsonDecodeError("Expected BsonInt64"))
  }

  implicit val string: BsonDecoder[String] = BsonDecoder.instance {
    case x: BsonString => Right(x.getValue)
    case _             => Left(BsonDecodeError("Expected BsonString"))
  }

  implicit val boolean: BsonDecoder[Boolean] = BsonDecoder.instance {
    case x: BsonBoolean => Right(x.getValue)
    case _              => Left(BsonDecodeError("Expected BsonBoolean"))
  }

  implicit val double: BsonDecoder[Double] = BsonDecoder.instance {
    case x: BsonDouble => Right(x.getValue)
    case _             => Left(BsonDecodeError("Expected BsonDouble"))
  }

  implicit val instant: BsonDecoder[Instant] = BsonDecoder.instance {
    case x: BsonDateTime => Right(Instant.ofEpochMilli(x.getValue))
    case _               => Left(BsonDecodeError("Expected BsonDateTime"))
  }

  implicit val bsonArray: BsonDecoder[BsonArray] = BsonDecoder.instance {
    case x: BsonArray => Right(x)
    case _            => Left(BsonDecodeError("Expected BsonArray"))
  }

  implicit val bsonBinary: BsonDecoder[BsonBinary] = BsonDecoder.instance {
    case x: BsonBinary => Right(x)
    case _             => Left(BsonDecodeError("Expected BsonBinary"))
  }

  implicit val bsonBoolean: BsonDecoder[BsonBoolean] = BsonDecoder.instance {
    case x: BsonBoolean => Right(x)
    case _              => Left(BsonDecodeError("Expected BsonBoolean"))
  }

  implicit val bsonDateTime: BsonDecoder[BsonDateTime] = BsonDecoder.instance {
    case x: BsonDateTime => Right(x)
    case _               => Left(BsonDecodeError("Expected BsonDateTime"))
  }

  implicit val bsonDbPointer: BsonDecoder[BsonDbPointer] = BsonDecoder.instance {
    case x: BsonDbPointer => Right(x)
    case _                => Left(BsonDecodeError("Expected BsonDbPointer"))
  }

  implicit val bsonJavaScript: BsonDecoder[BsonJavaScript] = BsonDecoder.instance {
    case x: BsonJavaScript => Right(x)
    case _                 => Left(BsonDecodeError("Expected BsonJavaScript"))
  }

  implicit val bsonJavaScriptWithScope: BsonDecoder[BsonJavaScriptWithScope] =
    BsonDecoder.instance {
      case x: BsonJavaScriptWithScope => Right(x)
      case _ => Left(BsonDecodeError("Expected BsonJavaScriptWithScope"))
    }

  implicit val bsonMaxKey: BsonDecoder[BsonMaxKey] = BsonDecoder.instance {
    case x: BsonMaxKey => Right(x)
    case _             => Left(BsonDecodeError("Expected BsonMaxKey"))
  }

  implicit val bsonMinKey: BsonDecoder[BsonMinKey] = BsonDecoder.instance {
    case x: BsonMinKey => Right(x)
    case _             => Left(BsonDecodeError("Expected BsonMinKey"))
  }

  implicit val bsonNull: BsonDecoder[BsonNull] = BsonDecoder.instance {
    case x: BsonNull => Right(x)
    case _           => Left(BsonDecodeError("Expected BsonNull"))
  }

  implicit val bsonNumber: BsonDecoder[BsonNumber] = BsonDecoder.instance {
    case x: BsonNumber => Right(x)
    case _             => Left(BsonDecodeError("Expected BsonNumber"))
  }

  implicit val bsonObjectId: BsonDecoder[BsonObjectId] = BsonDecoder.instance {
    case x: BsonObjectId => Right(x)
    case _               => Left(BsonDecodeError("Expected BsonObjectId"))
  }

  implicit val bsonRegex: BsonDecoder[BsonRegularExpression] = BsonDecoder.instance {
    case x: BsonRegularExpression => Right(x)
    case _                        => Left(BsonDecodeError("Expected BsonRegularExpression"))
  }

  implicit val bsonString: BsonDecoder[BsonString] = BsonDecoder.instance {
    case x: BsonString => Right(x)
    case _             => Left(BsonDecodeError("Expected BsonString"))
  }

  implicit val bsonSymbol: BsonDecoder[BsonSymbol] = BsonDecoder.instance {
    case x: BsonSymbol => Right(x)
    case _             => Left(BsonDecodeError("Expected BsonSymbol"))
  }

  implicit val bsonTimestamp: BsonDecoder[BsonTimestamp] = BsonDecoder.instance {
    case x: BsonTimestamp => Right(x)
    case _                => Left(BsonDecodeError("Expected BsonTimestamp"))
  }

  implicit val bsonUndefined: BsonDecoder[BsonUndefined] = BsonDecoder.instance {
    case x: BsonUndefined => Right(x)
    case _                => Left(BsonDecodeError("Expected BsonUndefined"))
  }

  implicit val bsonDecimal128: BsonDecoder[BsonDecimal128] = BsonDecoder.instance {
    case x: BsonDecimal128 => Right(x)
    case _                 => Left(BsonDecodeError("Expected BsonDecimal128"))
  }

  implicit val bsonDouble: BsonDecoder[BsonDouble] = BsonDecoder.instance {
    case x: BsonDouble => Right(x)
    case _             => Left(BsonDecodeError("Expected BsonDouble"))
  }

  implicit val bsonInt: BsonDecoder[BsonInt32] = BsonDecoder.instance {
    case x: BsonInt32 => Right(x)
    case _            => Left(BsonDecodeError("Expected BsonInt32"))
  }

  implicit val bsonLong: BsonDecoder[BsonInt64] = BsonDecoder.instance {
    case x: BsonInt64 => Right(x)
    case _            => Left(BsonDecodeError("Expected BsonInt64"))
  }

  implicit val bsonDocumentDecoder: BsonDecoder[BsonDocument] =
    BsonDecoder.instance[BsonDocument] {
      case bd: BsonDocument => Right(bd)
      case _                => Left(BsonDecodeError("Incorrect BsonDocument"))
    }

}
