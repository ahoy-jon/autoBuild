package org.apache.spark.sql

import java.lang

import io.univalence.centrifuge.Result
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, CreateArray, CreateStruct, ExprId, Expression, GenericRowWithSchema, GetStructField, Literal, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._

import scala.collection.{immutable, mutable}
import scala.reflect.runtime.universe.TypeTag


package object univalence {

  case class AnnotationSql(msg: String, isError: Boolean, count: Long, onField: String, fromFields: Seq[String])

  case class DeltaPart(colName:String,
                       sumOnlyLeft:Option[Long],
                       sumOnlyRight:Option[Long],
                       sumBothRight:Option[Long],
                       sumBothLeft:Option[Long],
                       sumBothDelta:Option[Long],
                       sumBothDeltaSquared:Option[Long],
                       countNbExact:Option[Long]) {

    def hasDifference:Boolean = {
      Seq(sumOnlyLeft.isDefined,
        sumOnlyRight.isDefined,
        !sumBothDeltaSquared.contains(0)).exists(identity)
    }
  }

  case class Delta(counts:DeltaPart,cols:Seq[DeltaPart])


  implicit class QATools(val sparkSession: SparkSession) {

    def registerTransformation[A: TypeTag, B: TypeTag](name: String, f: A => Result[B]): UserDefinedFunction = {

      val udf: UserDefinedFunction = sparkSession.udf.register("qa_raw_" + name, f)

      sparkSession.sessionState.functionRegistry.registerFunction(name,
        (s: Seq[Expression]) => GetStructField(
          child = udf.apply(s.map(Column.apply): _*).expr,
          ordinal = 0,
          name = Some("value")))

      udf
    }

  }

  private def cleanAnnotation(s: Seq[Any]): Seq[AnnotationSql] = {

    def cleanRow(a: Any): Seq[AnnotationSql] = {

      def cleanInerRow(aa: Any, onField: String, fromFields: Seq[String]): Seq[AnnotationSql] = {
        aa match {
          case m: Map[String, Any] => m.toSeq.flatMap(x => cleanInerRow(x._2, onField, fromFields))
          case row: GenericRowWithSchema => Seq(AnnotationSql(
            msg = row.getAs[String](0),
            isError = row.getAs[Boolean](1),
            count = row.getAs[Long](2),
            onField = onField, fromFields = fromFields))

          case wa: mutable.WrappedArray[Any] => wa.flatMap(x => cleanInerRow(x, onField, fromFields))
          case _ => println(aa.getClass + " : " + aa); Nil
        }
      }


      a match {
        case e: GenericRowWithSchema => cleanInerRow(e.get(2), e.getAs[String](1), e.getAs[Seq[String]](3))
      }
    }

    s.flatMap(cleanRow).groupBy(x => (x.fromFields,x.isError,x.msg,x.onField)).values.map(x => {
      x.head.copy(count = x.map(_.count).sum)
    }).toSeq
  }


  case class QAUdfInPlan(tocol: String, udf: ScalaUDF, fromFields: Seq[String])

  implicit class QADF(val dataFrame: DataFrame) {

    private def findColChildDeep(exp: Expression): Seq[String] = {
      exp match {
        case AttributeReference(name, _, _, _) => Seq(name)
        case _ => exp.children.toList match {
          case Nil => println(exp.getClass + " : " + exp); Nil
          case xs: List[Expression] => xs.flatMap(findColChildDeep)
        }
      }


    }

    private def findColChild(scalaUdf: ScalaUDF): Seq[String] = {
      scalaUdf.children.flatMap(findColChildDeep)
    }

    private def recursivelyFindScalaUDF(exp: Expression, tocol: String): Seq[QAUdfInPlan] = {
      exp match {
        case s: ScalaUDF => Seq(QAUdfInPlan(tocol, s, findColChild(s)))
        case _ => exp.children.flatMap(x => recursivelyFindScalaUDF(x, tocol))
      }
    }

    private def recursivelyFindScalaUDF(lp: LogicalPlan): Seq[QAUdfInPlan] = {
      lp.expressions.flatMap(x => {
        recursivelyFindScalaUDF(x, x.asInstanceOf[Alias].name)
      })
    }


    def includeSources: DataFrame = {

      val sparkSession = dataFrame.sparkSession

      val plan: LogicalPlan = dataFrame.queryExecution.optimizedPlan

      println(plan.toJSON)

      ???
    }

    def includeRejectFlags: DataFrame = {
      ???
    }

    def deltaWith(df:DataFrame):Delta = {

      assert(dataFrame.columns.toSeq == df.columns.toSeq)
      import org.apache.spark.sql.functions._

      val leftKey = dataFrame(dataFrame.columns.head)
      val rightKey = df(dataFrame.columns.head)
      val j = dataFrame.join(df,
        leftKey
          ===
          rightKey, "fullOuter")

      val valueCouple:Seq[(String,Column,Column)] =
        ("count1",Column(Literal(1)),Column(Literal(1))) :: dataFrame.columns.tail.map(x => {
          (x, dataFrame(x),df(x))
        }).toList

      val onlyLeft: Column = leftKey.isNotNull.&&(rightKey.isNull)
      val onlyRight: Column = leftKey.isNull.&&(rightKey.isNotNull)
      val both:Column = leftKey.isNotNull.&&(rightKey.isNotNull)

      type KpiApplier = (Column,Column) => (String,Column)

      val kpis:Seq[KpiApplier] = Seq[KpiApplier](
        (c1,_) => ("OnlyLeft", when(onlyLeft,c1)),
        (_,c2) => ("OnlyRight",when(onlyRight,c2)),
        (c1,_) => ("BothLeft", when(both,c1)),
        (_,c2) => ("BothRight",when(both,c2)),
        (c1,c2) => ("BothDelta",when(both,c1 - c2)),
        (c1,c2) => ("BothDeltaSquared",when(both,(c1 - c2).multiply(c1 - c2))),
        (c1,c2) => ("BothCountEqual", when(both && (c1 === c2),Column(Literal(1))))
      )

      val allCols: Seq[Column] = valueCouple.flatMap({case (n,c1,c2) => kpis.map(f => {
        val (id,c) = f(c1,c2)
        Column(Alias(sum(c).expr,n + "____" + id)())
      })})


      val r = j.select(allCols:_*).collect().head

      def deltaPart(fieldName:String):DeltaPart = {

        def e(kpi:String):Option[Long] = Option(r.getAs[Long](fieldName + "____" + kpi))

        DeltaPart(colName = fieldName,
          sumOnlyLeft = e("OnlyLeft"),
          sumOnlyRight = e("OnlyRight"),
          sumBothRight = e("BothRight"),
          sumBothLeft = e("BothLeft"),
          sumBothDelta = e("BothDelta"),
          sumBothDeltaSquared = e("BothDeltaSquared"),
          countNbExact = e("BothCountEqual"))
      }

      Delta(deltaPart("count1"),dataFrame.columns.tail.map(deltaPart))
    }


    def includeAnnotations: DataFrame = {

      val sparkSession = dataFrame.sparkSession

      val plan: LogicalPlan = dataFrame.queryExecution.optimizedPlan

      val collect: Seq[QAUdfInPlan] = recursivelyFindScalaUDF(plan)

      val annotationsDt =
        ArrayType(
          StructType(Seq(
            StructField("msg", StringType, false),
            StructField("isError", BooleanType, false),
            StructField("count", LongType, false),
            StructField("onField", StringType, false),
            StructField("fromFields", ArrayType(StringType, false), nullable = false))), false)

      val anns = if(collect.isEmpty) {
        Literal(ArrayData.toArrayData(Nil),annotationsDt)
      } else {

        val cleanAnnotationUDF = sparkSession.udf.register("qa_internal_clean_annotations", cleanAnnotation _)


        val array = CreateArray(
          collect.map(d => CreateStruct(Seq(
            Literal("udfinfo"),
            Literal(d.tocol),
            GetStructField(d.udf, 1, Some("annotations")),
            CreateArray(d.fromFields.distinct.sorted.map(x => Literal(x)))))))

        val cleaned: Column = cleanAnnotationUDF(Column(array))
       cleaned.expr
      }


      val anncol: NamedExpression = Alias(
        anns
        , "annotations")()


      val newPlan = plan match {
        case Project(projectList, child) =>
          Project(projectList :+ anncol, child)

        case _ => Project(Seq(UnresolvedStar(None),anncol),plan)

      }

      val ndf = new Dataset[Row](
        sqlContext = sparkSession.sqlContext,
        newPlan,
        RowEncoder(sparkSession.sessionState.executePlan(newPlan).analyzed.schema))

      ndf
    }
  }

}

