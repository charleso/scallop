package scallop.queue

import cats.{Functor, Monad, ~>}
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

case class Message[F[_], A](
  get: A,
  success: F[Unit],
  // We eventually might need a way to keep the message active
  // touch: FiniteDuration => F[Unit]
)

trait QueueW[F[_], A] {

  def send(message: A): F[Unit]
}

/**
 * An abstraction modelled on SQS, but not strictly required to be implemented with it.
 */
trait Queue[F[_], A] extends QueueW[F, A] { self =>

  def poll: F[List[Message[F, A]]]

  def pollError: F[List[Message[F, A]]]

  // NOTE: There is an Invariant class in cats, but it doesn't let you have failures on the contravariant side
  // https://typelevel.org/cats/typeclasses/invariant.html
  final def imap[B](f: A => Either[String, B])(g: B => A)(implicit F: Monad[F]): Queue[F, B] =
    new Queue[F, B] {
      override def poll: F[List[Message[F, B]]] =
        self.poll.flatMap(_
          .traverse(m => f(m.get).map(b => m.copy(get = b)).leftTraverse(_ => F.unit))
          .map(_.flatMap(_.toList))
        )

      override def pollError: F[List[Message[F, B]]] =
        self.pollError.flatMap(_
          .traverse(m => f(m.get).map(b => m.copy(get = b)).leftTraverse(_ => F.unit))
          .map(_.flatMap(_.toList))
        )

      override def send(message: B): F[Unit] =
        self.send(g(message))
    }

  final def mapF[G[_]: Functor](f: F ~> G): Queue[G, A] =
    new Queue[G, A] {
      override def poll: G[List[Message[G, A]]] =
        f(self.poll).map(_.map(m => m.copy(
          success = f(m.success),
        )))

      override def pollError: G[List[Message[G, A]]] =
        f(self.pollError).map(_.map(m => m.copy(
          success = f(m.success),
        )))

      override def send(message: A): G[Unit] =
        f(self.send(message))
    }
}

object Queue {

  def poll[F[_], A](
    q: Queue[F, A],
  )(io: A => F[Unit],
  )(implicit S: Monad[F],
  ): F[Unit] =
    pollX(q)(io.andThen(_.as(true)))

  def pollX[F[_], A](
    q: Queue[F, A],
  )(io: A => F[Boolean],
  )(implicit S: Monad[F],
  ): F[Unit] =
    pollRaw(q.poll)(io)

  def pollRaw[F[_], A](
    poll: F[List[Message[F, A]]]
  )(io: A => F[Boolean],
  )(implicit S: Monad[F],
  ): F[Unit] =
    pollRaw_(poll)(io).void

  def pollRaw_[F[_], A](
    poll: F[List[Message[F, A]]]
  )(io: A => F[Boolean],
  )(implicit S: Monad[F],
  ): F[List[A]] =
    for {
      as <- poll
      bs <- as.traverse(a =>
        io(a.get).flatMap(b => (if (b) a.success else S.unit).as(a.get))
      )
    } yield bs
}

case class QueueName(value: String) extends AnyVal

case class QueueProperties(
  maxRetries: Int,
  offsetDuration: FiniteDuration
)

object QueueProperties {

  def withTimeout(maxDuration: FiniteDuration, offsetDuration: FiniteDuration): QueueProperties =
    QueueProperties(maxDuration.div(offsetDuration).toInt, offsetDuration)
}
