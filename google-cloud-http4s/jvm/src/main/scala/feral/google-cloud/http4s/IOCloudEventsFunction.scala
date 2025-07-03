package feral.googlecloud

import cats.syntax.all._
import cats.effect.syntax.all._
import cats.effect.std.Dispatcher
import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.unsafe.IORuntime

import com.google.cloud.functions.CloudEventsFunction
import io.cloudevents.CloudEvent

import scala.util.control.NonFatal

abstract class IOCloudEventsFunction extends CloudEventsFunction {

  type CloudEventHandler[F[_]] = CloudEvent => F[Unit]

  protected def runtime: IORuntime = IORuntime.global
 
  def handler: Resource[IO, CloudEventHandler[IO]]

  private[this] val (dispatcher, handle) = {
    val handler = {
      val h =
        try this.handler
        catch { case ex if NonFatal(ex) => null }

      if (h ne null) {
        h.map(IO.pure(_))
      } else {
        val functionName = getClass().getSimpleName()
        val msg =
          s"""|There was an error initializing `$functionName` during startup.
              |Falling back to initialize-during-first-invocation strategy.
              |To fix, try replacing any `val`s in `$functionName` with `def`s.""".stripMargin
        System.err.println(msg)

        Async[Resource[IO, *]].defer(this.handler).memoize.map(_.allocated.map(_._1))
      }
    }  

    Dispatcher
      .parallel[IO](await = false)
      .product(handler)
      .allocated
      .map(_._1) // drop unused finalizer
      .unsafeRunSync()(runtime)
    }

  override def accept(event: CloudEvent): Unit = {
    handler.use(_.apply(event))
  }

}

