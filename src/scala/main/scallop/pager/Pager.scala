package scallop.pager

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import cats.data.EitherT
import cats.effect.Sync
import cats.{Functor, Monad, Monoid}
import cats.syntax.all._
import doobie.free.connection.ConnectionIO
import doobie.util.query.Query0
import doobie.util.transactor.Transactor
import doobie.implicits._
import doobie.util.Read
import doobie.util.fragment.Fragment

sealed abstract case class Limit(value: Int)

object Limit {

  def fromInt(value: Int): Either[IllegalArgumentException, Limit] =
    if (value < 1)
      Left(new IllegalArgumentException(s"Invalid limit, must be positive: $value"))
    else
      Right(new Limit(value) {})

  def fromIntUnsafe(value: Int): Limit =
    fromInt(value).valueOr(throw _)
}

sealed abstract case class Offset(value: Long)

object Offset {

  def fromLong(value: Long): Either[IllegalArgumentException, Offset] =
    if (value < 0)
      Left(new IllegalArgumentException(s"Invalid offset, cannot be negative: $value"))
    else
      Right(new Offset(value) {})

  def fromLongUnsafe(value: Long): Offset =
    fromLong(value).valueOr(throw _)
}

case class Total(value: Long)

object Cursor {

  def fromOffset(i: Offset): String =
    new String(Base64.getEncoder.encode(i.value.toString.getBytes(UTF_8)), UTF_8)

  def toOffset(s: String): Either[IllegalArgumentException, Offset] =
    try {
      Offset.fromLong(
        new String(Base64.getDecoder.decode(s.getBytes(UTF_8)), UTF_8).toLong
      )
    } catch {
      case e: IllegalArgumentException =>
        Left(e)
    }
}

case class PagerInput(
  limit: Option[Limit],
  offset: Option[Offset],
) {

  def limitSafe(max: Limit): Limit =
    limit.map(l => Limit.fromIntUnsafe(l.value.min(max.value))).getOrElse(max)
}

case class PagerResult[A](
  limit: Limit,
  offset: Option[Offset],
  total: Option[Total],
  result: A,
) {

  def map[B](f: A => B): PagerResult[B] =
    this.copy(result = f(this.result))
}

/**
 * http://www.alfredodinapoli.com/posts/2016-09-10-paginators-are-mealy-machines-in-disguise.html
 */
case class Pager[F[_], A](value: PagerInput => F[PagerResult[A]]) {

  def map[B](f: A => B)(implicit F: Functor[F]): Pager[F, B] =
    Pager(o => value(o).map(_.map(f)))

  /**
   * Allow consumers to evaluate values of the stream with effects.
   *
   * NOTE: There intentionally _isn't_ a `flatMap` for [[Pager]] as it doesn't make sense to page over multiple inputs.
   */
  def evalMap[B](f: A => F[B])(implicit F: Monad[F]): Pager[F, B] =
    Pager(o => value(o).flatMap(r =>
      f(r.result).map(x => r.copy(result = x))
    ))

  def transformF[G[_], B](f: F[PagerResult[A]] => G[PagerResult[B]]): Pager[G, B] =
    Pager(v => f(value(v)))

  def run(implicit F: Monad[F], M: Monoid[A]): F[A] = {
    runFrom(PagerInput(None, None))
  }

  def runFrom(i: PagerInput)(implicit F: Monad[F], M: Monoid[A]): F[A] = {
    runWith(i, M.empty)
  }

  def runOneAtATime(implicit F: Monad[F], M: Monoid[A]): F[A] =
    runFrom(PagerInput(Some(Limit.fromIntUnsafe(1)), None))

  def runWith(i: PagerInput, a: A)(implicit F: Monad[F], M: Monoid[A]): F[A] =
    value(i).flatMap(r =>
      if (r.offset.isEmpty) {
        M.combine(a, r.result).pure[F]
      } else {
        runWith(i.copy(offset = r.offset), M.combine(a, r.result))
      }
    )
}

object Pager {

  implicit class PagerConnectionIO[A](ma: Pager[ConnectionIO, A]) {

    def transact[F[_]: Sync](xa: Transactor[F]): Pager[F, A] =
      Pager.transact(ma, xa)
  }

  implicit class PagerConnectionEIO[E, A](ma: Pager[EitherT[ConnectionIO, E, *], A]) {

    def transact[F[_]: Sync](transactor: Transactor[F]): Pager[EitherT[F, E, *], A] =
      Pager(o => ma.value(o).transact(transactor))
  }

  implicit class PagerList[F[_], A](ma: Pager[F, List[A]]) {

    def mapValue[B](f: A => B)(implicit F: Monad[F]): Pager[F, List[B]] =
      Pager(o => ma.value(o).map(_.map(_.map(f))))

    def eval[B](f: A => F[B])(implicit F: Monad[F]): Pager[F, List[B]] =
      ma.evalMap(_.traverse(f))

    def eval_[B](f: A => F[Unit])(implicit F: Monad[F]): Pager[F, Unit] =
      ma.evalMap(_.traverse_(f))

    def option(implicit F: Functor[F]): F[Option[A]] =
      ma.value(PagerInput(Some(Limit.fromIntUnsafe(1)), None)).map(_.result.headOption)

    // Not strictly required but avoids having to import cats implicits
    def toList(implicit F: Monad[F]): F[List[A]] =
      ma.run
  }

  def transact[F[_]: Sync, A](p: Pager[ConnectionIO, A], transactor: Transactor[F]): Pager[F, A] =
    Pager(o => p.value(o).transact(transactor))

  def fromListF[F[_]: Functor, A](maxLimit: Limit, q: (Limit, Offset) => F[(Option[Total], List[A])]): Pager[F, List[A]] =
    Pager(i => {
      val l = i.limitSafe(maxLimit)
      val o = i.offset.getOrElse(Offset.fromLongUnsafe(0))
      // Request one more to avoid a guaranteed empty fetch when the results is the same length as limit
      q(Limit.fromIntUnsafe(l.value + 1), o).map { case (t, xs) =>
        val x = xs.take(l.value)
        PagerResult(
          l,
          if (xs.length <= l.value)
            None
          else
            Some(Offset.fromLongUnsafe(o.value + x.length)),
          t,
          x,
        )
      }
    })

  def fromFragment[A: Read](maxLimit: Limit, select: Fragment, q: Fragment): PagerDb[A] =
    PagerDb.fromFragment(maxLimit, select, q, total = true)
}

case class PagerDb[A](maxLimit: Limit, value: (Limit, Offset) => Query0[(Total, A)]) {

  def mapValue[B](f: A => B): PagerDb[B] =
    copy(value = (l, o) => value(l, o).map(_.map(f)))

  def toPager: Pager[ConnectionIO, List[A]] =
    Pager.fromListF(maxLimit, (l, o) => value(l, o).to[List].map(x =>
      (x.headOption.map(_._1).orElse(Some(Total(0))), x.map(_._2))
    ))

  def toList: ConnectionIO[List[A]] =
    toPager.toList
}

object PagerDb {

  def fromFragment[A: Read](maxLimit: Limit, select: Fragment, q: Fragment, total: Boolean): PagerDb[A] =
    PagerDb(maxLimit, (l, o) => (
        fr"SELECT " ++ (
          // https://stackoverflow.com/questions/28888375/run-a-query-with-a-limit-offset-and-also-get-the-total-number-of-rows
          // FIXME Make total optional?
          if (total) fr"count(*) OVER() AS full_count" else fr"0 :: bigint"
        ) ++ fr", " ++ select ++ q ++
        fr"LIMIT ${l.value.toLong} OFFSET ${o.value}"
      ).query[(Total, A)])
}
