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

import org.bson.{BsonDocument, BsonValue}

import Decoder.DecodeError

trait Decoder[A] extends Serializable { self =>
  def apply(b: BsonValue): Either[DecodeError, A]

  final def map[B](f: A => B) = new Decoder[B] {
    final def apply(b: BsonValue): Either[DecodeError, B] =
      self(b).map(f)
  }

  final def flatMap[B](f: A => Decoder[B]): Decoder[B] = new Decoder[B] {
    final def apply(b: BsonValue): Either[DecodeError, B] =
      self(b).flatMap(a => f(a)(b))
  }
}

trait DocumentDecoder[A] extends Serializable { self =>
  def apply(b: BsonDocument): Either[DecodeError, A]

  final def map[B](f: A => B) = new DocumentDecoder[B] {
    final def apply(b: BsonDocument): Either[DecodeError, B] =
      self(b).map(f)
  }

  final def flatMap[B](f: A => DocumentDecoder[B]): DocumentDecoder[B] = new DocumentDecoder[B] {
    final def apply(b: BsonDocument): Either[DecodeError, B] =
      self(b).flatMap(a => f(a)(b))
  }
}

object DocumentDecoder {
  def apply[A](implicit ev: DocumentDecoder[A]): DocumentDecoder[A] = ev
}

object Decoder {
  // NB: Not this
  type DecodeError = String

  def apply[A](implicit ev: Decoder[A]): Decoder[A] = ev
}
