/*
 * Copyright 2020 Precog Data
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

import cats.{Functor, Show}
import cats.data.NonEmptyList
import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import cats.mtl.FunctorRaise

import fs2.{text, Pipe, Stream}
import fs2.compress.gzip
import fs2.io.ssh.{Auth => SshAuth, Client, ConnectionConfig}

import org.slf4s.Logging

import quasar.api.{Column, ColumnType}
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.resource.ResourceName
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.destination.{Destination, LegacyDestination, ResultSink}
import quasar.connector.render.RenderConfig

import shims._

import scala.{Byte, None, Predef, Some, StringContext, Unit}, Predef._
import scala.util.Either

import java.lang.{RuntimeException, String, Throwable}
import java.net.InetSocketAddress
import java.time.format.DateTimeFormatter

final class TSDestination[F[_]: Concurrent: ContextShift: MonadResourceErr] private (
    isa: InetSocketAddress,
    config: TSConfig,
    client: Client[F],
    blocker: Blocker)
    extends LegacyDestination[F]
    with Logging {

  private val NullSentinel = "__sd_null_sentinel_str__"
  private val BufferSize = 1024 * 10    // keep in sync with BufferContext#RenderBufferSize

  private val cc =
    ConnectionConfig(
      isa,
      config.user,
      config.auth match {
        case Auth.PrivateKey(value, phrase) => SshAuth.KeyBytes(value.getBytes("UTF-8"), phrase)
        case Auth.Password(value) => SshAuth.Password(value)
      })

  def destinationType = TSDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F, Type]] =
    NonEmptyList.one(tsSink)

  private[this] val csvConfig =
    RenderConfig.Csv(
      includeHeader = false,
      nullSentinel = Some(NullSentinel),
      includeBom = false,
      offsetDateTimeFormat = DateTimeFormatter.ofPattern(MimirTimePatterns.LocalDateTime),    // TODO this is the time hack to make things work for now
      localDateTimeFormat = DateTimeFormatter.ofPattern(MimirTimePatterns.LocalDateTime),
      localDateFormat = DateTimeFormatter.ofPattern(MimirTimePatterns.LocalDate),
      localTimeFormat = DateTimeFormatter.ofPattern(MimirTimePatterns.LocalTime))

  private[this] val tsSink: ResultSink[F, Type] =
    ResultSink.create[F, Type](csvConfig) { (path, columns, bytes) =>
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
        tableName <- Stream.eval[F, String](tableNameF)

        _ <- Stream.eval(
            Sync[F].delay(
              log.info(s"(re)creating ${config.database}.${tableName} with schema ${columns.show}")))

        p <- Stream.resource(client.exec(cc, "tql", blocker))
        _ <- Stream(overwriteDdl(tableName, columns) + "exit;")
          .flatMap(s => Stream.emits(s.getBytes("UTF-8")))
          .through(p.stdin)

        cmd = loadCommand(tableName)
        _ <- Stream.eval(
          Sync[F].delay(
            log.info(s"running remote ingest: $cmd")))

        p <- Stream.resource(client.exec(cc, cmd, blocker))

        exitCode <- bytes
          // .observe(in => text.utf8Decode(in).through(text.lines).through(fs2.Sink.showLinesStdOut))
          .observe(chunkLogSink)
          .through(gzip[F](BufferSize))
          .through(p.stdin)
          .concurrently(
            p.stdout
              .through(text.utf8Decode)
              .through(text.lines)
              .merge(
                p.stderr
                  .through(text.utf8Decode)
                  .through(text.lines))
              .through(infoSink)).drain ++ Stream.eval(p.join)

        _ <- if (exitCode =!= 0)
          Stream.eval(Sync[F].delay(log.warn(s"tsload exited with status $exitCode")) *>
            Sync[F].raiseError(TSDestination.IngestFailure: Throwable))
        else
          Stream.eval(Sync[F].delay(log.info(s"tsload exited with status $exitCode")))
      } yield ()

      r handleErrorWith { t =>
        Stream.eval(
          Sync[F].delay(log.error("thoughtspot push produced unexpected error", t)) >>
            Sync[F].raiseError(t))
      }
    }

  private[this] def loadCommand(tableName: String): String =
    s"""
    | gzip -dc |
    | tsload
    |   --target_database '${config.database}'
    |   ${config.schema.map(s => s"""--target_schema "$s"""").getOrElse("")}
    |   --target_table '$tableName'
    |   --max_ignored_rows 8192
    |   --field_separator ','
    |   --null_value '$NullSentinel'
    |   --date_time_format '${TSTimePatterns.LocalDateTime}'
    |   --date_format '${TSTimePatterns.LocalDate}'
    |   --time_format '${TSTimePatterns.LocalTime}'
    |   --skip_second_fraction
    |   --empty_target
    |   --boolean_representation 'true_false'""".stripMargin.replace("\n", "")

  // TODO partitioning
  private[this] def overwriteDdl(tableName: String, columns: NonEmptyList[Column[ColumnType.Scalar]]): String = {
    def renderColumn(col: Column[ColumnType.Scalar]): String = {
      import ColumnType.{String => _, _}

      val Column(name, tpe) = col

      val tpeStr = tpe match {
        case Boolean | Null => "BOOL"
        case OffsetTime => ???
        case LocalTime => "TIME"
        case OffsetDate => ???
        case LocalDate => "DATE"
        // case OffsetDateTime => ???
        case OffsetDateTime | LocalDateTime => "DATETIME"
        case Interval => ???
        case Number => "DOUBLE"   // TODO
        case ColumnType.String => "VARCHAR(1024)"   // TODO!
      }

      s""""${name}" $tpeStr"""
    }

    val colsStr = columns.map(renderColumn).toList.mkString("(", ",", ")")

    s"""USE "${config.database}";
      | ${config.schema.map(s => s"""CREATE SCHEMA "$s";""")};
      | DROP TABLE "$tableName";
      | CREATE TABLE "$tableName" $colsStr;
      """.stripMargin
  }

  private[this] def infoSink[A: Show]: Pipe[F, A, Unit] =
    _.evalMap(a => Sync[F].delay(log.trace(a.show)))

  private[this] def traceSink[A: Show]: Pipe[F, A, Unit] =
    _.evalMap(a => Sync[F].delay(log.trace(a.show)))

  private[this] val chunkLogSink: Pipe[F, Byte, Unit] =
    _.chunks.map(c => s"sending ${c.size} bytes").through(traceSink)

  private object TSTimePatterns {
    val LocalDate = "%Y-%m-%d"
    val LocalTime = "%H:%M:%S"
    val LocalDateTime = s"${LocalDate} ${LocalTime}"
  }

  private object MimirTimePatterns {
    val LocalDate = "yyyy-MM-dd"
    val LocalTime = "HH:mm:ss"
    val LocalDateTime = s"${LocalDate} ${LocalTime}"
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

  case object IngestFailure extends RuntimeException("ThoughtSpot ingest failed for unknown reasons; check the server logs")
}
