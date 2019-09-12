/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.physical.ts

import argonaut._, Argonaut._

import cats.implicits._

import scala.{Int, Product, Serializable}

import java.lang.String

final case class TSConfig(
    host: String,
    port: Int,
    user: String,
    auth: Auth,
    database: String) {

  def sanitized: TSConfig =
    copy(auth = auth.sanitized)
}

object TSConfig {
  implicit val codec: CodecJson[TSConfig] =
    casecodec5(TSConfig.apply, TSConfig.unapply)("host", "port", "user", "auth", "database")
}

sealed trait Auth extends Product with Serializable {
  def sanitized: Auth = this match {
    case Auth.PrivateKey(_) => Auth.PrivateKey("<redacted>")
    case Auth.Password(_) => Auth.Password("<redacted>")
  }
}

object Auth {

  implicit val codec: CodecJson[Auth] = {
    CodecJson(
      {
        case PrivateKey(value) =>
          ("type" := PrivateKey.Key) ->: ("contents" := value) ->: jEmptyObject

        case Password(value) =>
          ("type" := Password.Key) ->: ("contents" := value) ->: jEmptyObject
      },
      { json =>
        for {
          tpe <- (json --\ "type").as[String]
          contents <- (json --\ "contents").as[String]

          back <- if (tpe === PrivateKey.Key)
            DecodeResult.ok(PrivateKey(contents))
          else if (tpe === Password.Key)
            DecodeResult.ok(Password(contents))
          else
            DecodeResult.fail("type must be one of: 'private-key', 'password'", json.history)
        } yield back
      })
  }

  final case class PrivateKey(value: String) extends Auth

  object PrivateKey extends (String => Auth) {
    val Key = "private-key"
  }

  final case class Password(value: String) extends Auth

  object Password extends (String => Auth) {
    val Key = "password"
  }
}
