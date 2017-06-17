package io.univalence.centrifuge.sql

import io.univalence.centrifuge.Result
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.univalence.AnnotationSql
import org.scalatest.FunSuite

case class Person(name: String, age: Int)

case class PersonWithAnnotations(name: String, age: Int, annotations: Seq[AnnotationSql])

object AnnotationTest {
  def to_age(i: Int): Result[Int] = {
    {} match {
      case _ if i < 0 => Result.fromError("BELOW_ZERO")
      case _ if i <= 13 => Result.fromWarning(i, "UNDER_13")
      case _ if i >= 130 => Result.fromError("OVER_130")
      case _ => Result.pure(i)
    }
  }

  def non_empty_string(str: String): Result[String] = {
    str match {
      //case None => Result.fromError("NULL_VALUE")
      case "" => Result.fromError("EMPTY_STRING")
      case _ => Result.pure(str)
    }
  }

}

class AnnotationTest extends FunSuite {

  val ss: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  import ss.implicits._

  val onePersonDf = ss.sparkContext.makeRDD(Seq(Person("Joe", 14))).toDF()

  import org.apache.spark.sql.univalence._


  test("explore") {


    val select = onePersonDf.select("*")

    println(select.queryExecution.toString())
    select.queryExecution.logical.expressions.foreach(x => {
      println(x);
      println(x.prettyName)
    })

  }

  test("basic") {
    val p = onePersonDf.includeAnnotations.as[PersonWithAnnotations].collect().head

    assert(p.annotations.isEmpty)
    assert(p.name == "Joe")
    assert(p.age == 14)
  }


  test("with one annotation") {
    ss.registerTransformation("to_age", AnnotationTest.to_age)
    ss.sparkContext.makeRDD(Seq(Person("Joe", 12))).toDS().createTempView("person")

    val p: Option[(Int, Seq[AnnotationSql])] = ss.sql("select to_age(age) as age from person")
      .includeAnnotations
      .as[(Int, Seq[AnnotationSql])]
      .collect()
      .headOption

    assert(p.isDefined)
    assert(p.get._1 == 12)
    assert(p.get._2 == Seq(
      AnnotationSql(
        msg = "UNDER_13",
        isError = false,
        count = 1,
        onField = "age",
        fromFields = Seq("age"))))
  }

  test("with more annotations (same field)") {
    ss.registerTransformation("to_age", AnnotationTest.to_age)
    ss.sparkContext.makeRDD(Seq(Person("Joe", 12))).toDS().createOrReplaceTempView("person")

    val p: Option[(Int, Seq[AnnotationSql])] = ss.sql("select to_age(to_age(age) + to_age(age) - to_age(age)) as age from person")
      .includeAnnotations.as[(Int, Seq[AnnotationSql])]
      .collect()
      .headOption

    assert(p.isDefined)
    assert(p.get._1 == 12)
    assert(p.get._2.toSet == Set(AnnotationSql("UNDER_13", false, 4, "age", Seq("age"))))
  }

  test("multicol") {

    ss.registerTransformation("to_age", AnnotationTest.to_age)
    ss.registerTransformation("non_empty", AnnotationTest.non_empty_string)

    ss.sparkContext.makeRDD(Seq(Person("", -1))).toDS().createOrReplaceTempView("person")

    val res = ss.sql("select to_age(age) as person_age, non_empty(name) as person_name from person").includeAnnotations.as[(Option[Int], Option[String], Seq[AnnotationSql])].collect().head

    assert(res._1.isEmpty)
    assert(res._2.isEmpty)
    assert(res._3.toSet == Set(
      AnnotationSql(
        msg = "BELOW_ZERO",
        isError = true,
        count = 1,
        onField = "person_age",
        fromFields = Seq("age")),
      AnnotationSql(
        msg = "EMPTY_STRING",
        isError = true,
        count = 1,
        onField = "person_name",
        fromFields = Seq("name")))
    )
  }

  test("") {


  }

  test("ajout du flag") {


  }

  test("ajout des causality cols") {


  }
  test("ajout du transformation name") {

  }

  test("group by") {


  }

  test("sub select") {

  }

  test("rename + ajout des sources") {

  }

  test("deltaQA 2 df") {


    val df1 = ss.sparkContext.makeRDD(Seq(("abc",2,3),("def",13,17))).toDF()

    val df2 = ss.sparkContext.makeRDD(Seq(("abc",5,7))).toDF()

   val delta = df1.deltaWith(df2)

    assert(delta.cols.forall(_.hasDifference))
  }

  test("delta QA same DF") {
    val df = ss.sparkContext.makeRDD(Seq(("abc",2,3),("def",13,17))).toDF()

    assert(df.deltaWith(df).cols.forall(!_.hasDifference))

  }


}
