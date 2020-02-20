/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef._

import argonaut.{Argonaut, Json}, Argonaut._

import cats.effect.{ConcurrentEffect, ContextShift, Resource,   Timer}

import eu.timepit.refined.auto._

import quasar.api.destination.{DestinationError, DestinationType}, DestinationError.InitializationError
import quasar.api.destination.DestinationError.InitializationError
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Destination, DestinationModule}

import scala.util.Either

object TSDestinationModule extends DestinationModule {

  def destinationType = DestinationType("thoughtspot", 1L)

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[TSConfig].map(_.sanitized.asJson).getOr(jEmptyObject)

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json)
      : Resource[F, Either[InitializationError[Json], Destination[F]]] =
    config.as[TSConfig].fold(
      (msg, _) =>
        Resource.pure(
          Left(DestinationError.MalformedConfiguration(destinationType, config, msg)): Either[InitializationError[Json], Destination[F]]),
      TSDestination[F, Json](_))
}
