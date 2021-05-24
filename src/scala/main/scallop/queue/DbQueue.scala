package scallop.queue

import cats.effect.Sync
import doobie.{ConnectionIO, Transactor}
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.meta.Meta
import doobie.util.query.Query0
import doobie.util.update.Update0
import org.postgresql.util.PGInterval

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

object DbQueue {

  // FIXME Not sure why this is still required, the issue was raised here
  // https://github.com/tpolecat/doobie/issues/1095
  // This fix was closed, but it works, and actual fix in 0.8.8 doesn't seem to
  // https://github.com/tpolecat/doobie/pull/1097
  implicit val JavaTimeInstantMeta: Meta[java.time.Instant] =
    doobie.implicits.javasql.TimestampMeta.imap(_.toInstant)(java.sql.Timestamp.from)

  type Key = String

  sealed trait State {

    def db: Int =
      this match {
        case State.Queue =>
          0
        case State.Error =>
          1
        case State.Unknown =>
          2
        case State.Success =>
          3
        case State.SuccessError =>
          4
      }
  }

  object State {
    case object Queue extends State
    case object Error extends State
    case object Unknown extends State
    case object Success extends State
    case object SuccessError extends State

    def fromDb(s: Int): State =
      s match {
        case 0 => Queue
        case 1 => Error
        case 2 => Unknown
        case 3 => Success
        case 4 => SuccessError
        case _ => Unknown
      }
  }

  case class Entry(
    name: QueueName,
    state: State,
    created: Instant,
    updated: Instant,
  )


  implicit class DbQueueOps[A](q: Queue[ConnectionIO, A]) {

     def transact[F[_]: Sync](transactor: Transactor[F]): Queue[F, A] =
       q.mapF(transactor.trans)
  }

  def create(name: QueueName, fetchSize: Int, properties: QueueProperties): Queue[ConnectionIO, Key] =
    new Queue[ConnectionIO, Key] {

      override def poll: ConnectionIO[List[Message[ConnectionIO, Key]]] =
        fetch(name, fetchSize, properties.maxRetries, properties.offsetDuration, State.Queue, None).to[List].map(_.map {
          case (id, data) =>
            Message(
              data,
              success(name, id, State.Success).run.map((_: Int) => ()),
            )
        })

      override def pollError: ConnectionIO[List[Message[ConnectionIO, Key]]] =
        fetch(name, fetchSize, properties.maxRetries, properties.offsetDuration, State.Error, None).to[List].map(_.map {
          case (id, data) =>
            Message(
              data,
              success(name, id, State.SuccessError).run.map((_: Int) => ()),
            )
        })

      override def send(message: Key): ConnectionIO[Unit] =
        insert(name, message, None).run.map((_: Int) => ())
    }

  def insert(name: QueueName, data: Key, now: Option[Instant]): Update0 = {
    val sql =
      sql"""
        INSERT INTO queue (
          name,
          state,
          key,
          next
        ) VALUES (
          ${name},
          0,
          ${data},
          COALESCE($now, NOW())
        )
      """
    sql.update
  }

  def success(name: QueueName, id: Long, state: State): Update0 = {
    val sql =
      sql"""
        UPDATE
          queue
        SET
          state = ${state.db}
        WHERE
          name = ${name} AND id = ${id}
      """
    sql.update
  }

  def fetch(name: QueueName, limit: Int, maxRetries: Int, offset: FiniteDuration, state: State, now: Option[Instant]): Query0[(Long, Key)] = {
    val stateNext = state match {
      case State.Queue => State.Error
      case _ => State.Unknown
    }
    val sql = sql"""
      UPDATE
        queue
      SET
        next = CASE WHEN retries < ${maxRetries} THEN COALESCE($now, NOW()) + ${new PGInterval(offset.toSeconds.toString + " seconds")} ELSE COALESCE($now, NOW()) END,
        state = CASE WHEN retries < ${maxRetries} THEN state ELSE ${stateNext.db} END,
        retries = retries + 1
      WHERE
        id IN (
          SELECT
            id
          FROM
            queue
          WHERE
              name = ${name}
            AND
              state = ${state.db}
            AND
              next <= COALESCE($now, NOW())
          ORDER BY
            next ASC
          FOR UPDATE SKIP LOCKED
          LIMIT
            ${limit}
        )
      RETURNING
        id,
        key
      """
    sql.query
  }

  def statsMap: ConnectionIO[Map[QueueName, (Long, Long)]] =
    stats.to[List].map(_.toMap)

  def stats: Query0[(QueueName, (Long, Long))] = {
    val sql =
      sql"""
        SELECT
          name,
          count(CASE WHEN state = 0 THEN 1 END) AS count_queue,
          count(CASE WHEN state = 1 THEN 1 END) AS count_error
        FROM
          queue
        WHERE
          state = 0 OR state = 1
        GROUP BY
          name, state
      """
    sql.query
  }

  def log(key: Key): Query0[Entry] = {
    val sql =
      sql"""
        SELECT
          name,
          state,
          created_at,
          next
        FROM
          queue
        WHERE
          key = ${key}
        ORDER BY
          created_at
      """
    sql.query[(QueueName, Int, Instant, Instant)].map {
      case (n, s, ic, in) =>
        Entry(n, State.fromDb(s), ic, in)
    }
  }
}
