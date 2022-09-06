package com.example.ksqldb

trait FutureConverter {

  import scala.concurrent.{ Future, Promise }
  import scala.util.Try

  implicit class FutureConverter[T](jFuture: java.util.concurrent.Future[T]) {

    def toScalaFuture: Future[T] = {
      val promise = Promise[T]()
      new Thread(() =>
        promise.complete(Try {
          jFuture.get
        })
      ).start()
      promise.future
    }

  }

}
