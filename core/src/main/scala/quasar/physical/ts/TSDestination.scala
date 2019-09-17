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

import cats.Functor
import cats.effect.{Blocker, Concurrent, ContextShift, Resource}
import cats.implicits._
import cats.mtl.FunctorRaise

import fs2.Stream
import fs2.compress.gzip
import fs2.io.ssh.{Auth => SshAuth, Client, ConnectionConfig}

import quasar.api.destination.{Destination, DestinationError, ResultSink}, DestinationError.InitializationError
import quasar.api.resource.ResourceName
import quasar.api.table.{ColumnType, TableColumn}
import quasar.connector.{MonadResourceErr, ResourceError}

import scalaz.NonEmptyList

import shims._

import scala.{List, None, Predef, Some, StringContext}, Predef._
import scala.util.Either

import java.lang.String
import java.net.InetSocketAddress

final class TSDestination[F[_]: Concurrent: ContextShift: MonadResourceErr] private (
    isa: InetSocketAddress,
    config: TSConfig,
    client: Client[F],
    blocker: Blocker)
    extends Destination[F] {

  private val BufferSize = 1024 * 10    // keep in sync with BufferContext#RenderBufferSize

  private val cc =
    ConnectionConfig(
      isa,
      config.user,
      config.auth match {
        case Auth.PrivateKey(value) => SshAuth.KeyBytes(value.getBytes("UTF-8"))
        case Auth.Password(value) => SshAuth.Password(value)
      })

  def destinationType = TSDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F]] =
    NonEmptyList(tsSink)

  private[this] val tsSink: ResultSink[F] =
    ResultSink.Csv[F] { (path, columns, bytes) =>
      implicit val raiseClientErrorInResourceErr: FunctorRaise[F, Client.Error] =
          new FunctorRaise[F, Client.Error] {
            val functor = Functor[F]
            def raise[A](err: Client.Error): F[A] = err match {
              case Client.Error.Authentication =>
                MonadResourceErr[F].raiseError[A](
                  ResourceError.AccessDenied(
                    path,
                    Some(s"unable to authenticate with ssh server: user = ${config.user}"),
                    None))
            }
          }

      val tableNameF = path.uncons match {
        case Some((ResourceName(name), _)) =>
          name.pure[F]

        case _ =>
          MonadResourceErr[F].raiseError[String](
            ResourceError.MalformedResource(
              path,
              "path must contain exactly one component",
              None,
              None))
      }

      val r = for {
        tableName <- Resource.liftF[F, String](tableNameF)

        p <- client.exec(cc, "tql", blocker)
        _ <- Stream(overwriteDdl(tableName, columns) + "exit;")
          .flatMap(s => Stream.emits(s.getBytes("UTF-8")))
          .through(p.stdin)
          .compile
          .resource
          .drain

        p <- client.exec(cc, loadCommand(tableName), blocker)
        _ <- bytes
          .through(gzip[F](BufferSize))
          .through(p.stdin)
          .compile
          .resource
          .drain
      } yield ()

      r.use(_.pure[F])
    }

  private[this] def loadCommand(table: String): String =
    s"""
    | gzip -dc |
    | tsload
    |   --target_database '${config.database}'
    |   --target_table '$table'
    |   --field_separator ','
    |   --null_value ''
    |   --date_time_format '%Y-%m-%d %H:%M:%S'
    |   --skip_second_fraction
    |   --date_format '%Y-%m-%d'
    |   --boolean_representation 'true'""".stripMargin.replace("\n", "")

  // TODO partitioning
  private[this] def overwriteDdl(tableName: String, columns: List[TableColumn]): String = {
    def renderColumn(col: TableColumn): String = {
      import ColumnType.{String => _, _}

      val TableColumn(name, tpe) = col

      val tpeStr = tpe match {
        case Boolean | Null => "BOOL"
        case LocalTime | OffsetTime => "TIME"
        case LocalDate | OffsetDate => "DATE"
        case LocalDateTime | OffsetDateTime => "DATETIME"
        case Interval => ???
        case Number => "DOUBLE"   // TODO
        case ColumnType.String => "VARCHAR(255)"   // TODO!
      }

      s""""${name}" $tpeStr"""
    }

    val colsStr = columns.map(renderColumn).mkString("(", ",", ")")

    s"""USE "${config.database}";
      | DROP TABLE "$tableName";
      | CREATE TABLE "$tableName" $colsStr;
      """.stripMargin
  }
}

object TSDestination {

  def apply[F[_]: Concurrent: ContextShift: MonadResourceErr, C](
      config: TSConfig)
      : Resource[F, Either[InitializationError[C], Destination[F]]] =
    for {
      blocker <- Blocker[F]
      isa <- Resource.liftF(Client.resolve[F](config.host, config.port, blocker))
      client <- Client[F]
    } yield new TSDestination[F](isa, config, client, blocker).asRight[InitializationError[C]]
}
