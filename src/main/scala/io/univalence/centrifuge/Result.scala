package io.univalence.centrifuge

import org.apache.spark.sql.Annotation


case class Result[+T](value:Option[T],annotations:Seq[Annotation])

object Result {


  def pure[T](t:T):Result[T] = Result(Some(t),Nil)
  def fromError(error:String) = Result(None,Seq(Annotation.fromString(s = error,error = true)))
  def fromWarning[T](t:T,warning:String) = Result(Some(t),Seq(Annotation.fromString(s = warning,error = false)))
}
