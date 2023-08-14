package com.clairvoyant.data.scalaxy.transformer

import com.clairvoyant.data.scalaxy.test.util.DataScalaxyTestUtil
import com.clairvoyant.data.scalaxy.transformer.DataFrameTransformerImplicits.*
import org.apache.spark.sql.types.*

class DataFrameTransformerImplicitsSpec extends DataScalaxyTestUtil {

  "addColumn() - without data type" should "add new column with default data type" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": 10.2
        |}
        |""".stripMargin
    )

    val actualDF = df.addColumn(
      columnName = "col_D",
      columnValue = "val_D"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": 10.2,
        |  "col_D": "val_D"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "addColumn() - with data type" should "add new column with casted data type" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": 10.2
        |}
        |""".stripMargin
    )

    val actualDF = df.addColumn(
      columnName = "col_D",
      columnValue = "5.2",
      columnDataType = Some("double")
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": 10.2,
        |  "col_D": 5.2
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "addColumnWithExpression() - without data type" should "add new column with default data type" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": 10.2
        |}
        |""".stripMargin
    )

    val actualDF = df.addColumnWithExpression(
      columnName = "col_D",
      columnExpression = "col_C * 2"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": 10.2,
        |  "col_D": 20.4
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "addColumnWithExpression() - with data type" should "add new column with casted data type" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": 10.2
        |}
        |""".stripMargin
    )

    val actualDF = df.addColumnWithExpression(
      columnName = "col_D",
      columnExpression = "col_C * 2",
      columnDataType = Some("long")
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": 10.2,
        |  "col_D": 20
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "addPrefixToColumnNames() - with empty column list" should "add prefix to all columns" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": "val_C"
        |}
        |""".stripMargin
    )

    val actualDF = df.addPrefixToColumnNames(
      prefix = "test"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "test_col_A": "val_A",
        |  "test_col_B": "val_B",
        |  "test_col_C": "val_C"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "addPrefixToColumnNames() - with non empty column list" should "add prefix to specified columns in the list" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": "val_C"
        |}
        |""".stripMargin
    )

    val actualDF = df.addPrefixToColumnNames(
      prefix = "test",
      columnNames = List("col_A", "col_B")
    )

    val expectedDF = readJSON(
      """
        |{
        |  "test_col_A": "val_A",
        |  "test_col_B": "val_B",
        |  "col_C": "val_C"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "addSuffixToColumnNames() - with empty column list" should "add suffix to all columns" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": "val_C"
        |}
        |""".stripMargin
    )

    val actualDF = df.addSuffixToColumnNames(
      suffix = "test"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col_A_test": "val_A",
        |  "col_B_test": "val_B",
        |  "col_C_test": "val_C"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "addSuffixToColumnNames() - with non empty column list" should "add suffix to specified columns in the list" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": "val_C"
        |}
        |""".stripMargin
    )

    val actualDF = df.addSuffixToColumnNames(
      suffix = "test",
      columnNames = List("col_A", "col_B")
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col_A_test": "val_A",
        |  "col_B_test": "val_B",
        |  "col_C": "val_C"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "castColumns() - with empty data type mapper" should "not cast any column" in {
    val df = readJSON(
      """
        |{
        |  "col_A": 5,
        |  "col_B": 4,
        |  "col_C": 3.4678,
        |  "col_D": "1990-07-23 10:20:30",
        |  "col_E": "23-07-1990 10:20:30",
        |  "col_F": "1990-07-23",
        |  "col_G": "23-07-1990"
        |}
        |""".stripMargin
    )

    val transformedDF = df.castColumns(
      columnDataTypeMapper = Map[String, String]()
    )

    transformedDF.schema.fields
      .filter(_.name == "col_A")
      .head
      .dataType shouldBe LongType

    transformedDF.schema.fields
      .filter(_.name == "col_B")
      .head
      .dataType shouldBe LongType

    transformedDF.schema.fields
      .filter(_.name == "col_C")
      .head
      .dataType shouldBe DoubleType

    transformedDF.schema.fields
      .filter(_.name == "col_D")
      .head
      .dataType shouldBe StringType

    transformedDF.schema.fields
      .filter(_.name == "col_E")
      .head
      .dataType shouldBe StringType

    transformedDF.schema.fields
      .filter(_.name == "col_F")
      .head
      .dataType shouldBe StringType

    transformedDF.schema.fields
      .filter(_.name == "col_G")
      .head
      .dataType shouldBe StringType
  }

  "castColumns() - with non empty data type mapper" should "cast columns as specified in the mapper" in {
    val df = readJSON(
      """
        |{
        |  "col_A": 5,
        |  "col_B": 4,
        |  "col_C": 3.4678,
        |  "col_D": "1990-07-23 10:20:30",
        |  "col_E": "23-07-1990 10:20:30",
        |  "col_F": "1990-07-23",
        |  "col_G": "23-07-1990"
        |}
        |""".stripMargin
    )

    val transformedDF = df.castColumns(
      columnDataTypeMapper = Map(
        "col_A" -> "string",
        "col_B" -> "double",
        "col_C" -> "decimal(19, 2)",
        "col_D" -> "timestamp",
        "col_E" -> "timestamp(dd-MM-yyyy HH:mm:ss)",
        "col_F" -> "date",
        "col_G" -> "date(dd-MM-yyyy)"
      )
    )

    transformedDF.schema.fields
      .filter(_.name == "col_A")
      .head
      .dataType shouldBe StringType

    transformedDF.schema.fields
      .filter(_.name == "col_B")
      .head
      .dataType shouldBe DoubleType

    transformedDF.schema.fields
      .filter(_.name == "col_C")
      .head
      .dataType shouldBe DecimalType(19, 2)

    transformedDF.schema.fields
      .filter(_.name == "col_D")
      .head
      .dataType shouldBe TimestampType

    transformedDF.schema.fields
      .filter(_.name == "col_E")
      .head
      .dataType shouldBe TimestampType

    transformedDF.schema.fields
      .filter(_.name == "col_F")
      .head
      .dataType shouldBe DateType

    transformedDF.schema.fields
      .filter(_.name == "col_G")
      .head
      .dataType shouldBe DateType
  }

  "castColumnsBasedOnPrefix() - with prefix and data type" should "cast columns having specifed prefix" in {
    val df = readJSON(
      """
        |{
        |  "name": "abc",
        |  "price_in_india": "240",
        |  "price_in_canada": "3",
        |  "percent_difference": "10.23"
        |}
        |""".stripMargin
    )

    val transformedDF = df.castColumnsBasedOnPrefix(
      prefix = "price",
      dataType = "float"
    )

    transformedDF.schema.fields
      .filter(_.name == "name")
      .head
      .dataType shouldBe StringType

    transformedDF.schema.fields
      .filter(_.name == "price_in_india")
      .head
      .dataType shouldBe FloatType

    transformedDF.schema.fields
      .filter(_.name == "price_in_canada")
      .head
      .dataType shouldBe FloatType

    transformedDF.schema.fields
      .filter(_.name == "percent_difference")
      .head
      .dataType shouldBe StringType
  }

  "castColumnsBasedOnSuffix() - with suffix and data type" should "cast columns having specifed suffix" in {
    val df = readJSON(
      """
        |{
        |  "name": "abc",
        |  "india_price": "240",
        |  "US_price": "3",
        |  "percent_difference": "10.23"
        |}
        |""".stripMargin
    )

    val transformedDF = df.castColumnsBasedOnSuffix(
      suffix = "price",
      dataType = "float"
    )

    transformedDF.schema.fields
      .filter(_.name == "name")
      .head
      .dataType shouldBe StringType

    transformedDF.schema.fields
      .filter(_.name == "india_price")
      .head
      .dataType shouldBe FloatType

    transformedDF.schema.fields
      .filter(_.name == "US_price")
      .head
      .dataType shouldBe FloatType

    transformedDF.schema.fields
      .filter(_.name == "percent_difference")
      .head
      .dataType shouldBe StringType
  }

  "castFromToDataTypes() - with castRecursively as true" should "cast data types in nested manner" in {
    val df = readJSON(
      """
        |{
        |  "col_A": 5,
        |  "col_B": 4,
        |  "col_C": 3.4678,
        |  "col_D": {
        |     "col_E": 6
        |   },
        |  "col_F": [
        |    {
        |       "col_G": 4.356343
        |    }
        |  ]
        |}
        |""".stripMargin
    )

    val transformedDF = df.castFromToDataTypes(
      dataTypeMapper = Map(
        "long" -> "integer",
        "double" -> "decimal(5, 2)"
      ),
      castRecursively = true
    )

    transformedDF.schema.fields
      .filter(_.name == "col_A")
      .head
      .dataType shouldBe IntegerType

    transformedDF.schema.fields
      .filter(_.name == "col_B")
      .head
      .dataType shouldBe IntegerType

    transformedDF.schema.fields
      .filter(_.name == "col_C")
      .head
      .dataType shouldBe new DecimalType(5, 2)

    transformedDF
      .select("col_D.col_E")
      .schema
      .fields
      .head
      .dataType shouldBe IntegerType

    (
      transformedDF.schema
        .filter(_.name == "col_F")
        .head
        .dataType match {
        case ArrayType(nestedArrayType: StructType, _) =>
          nestedArrayType
            .filter(_.name == "col_G")
            .head
            .dataType
      }
    ) shouldBe new DecimalType(5, 2)
  }

  "castFromToDataTypes() - with castRecursively as false" should "cast data types only at root level" in {
    val df = readJSON(
      """
        |{
        |  "col_A": 5,
        |  "col_B": 4,
        |  "col_C": 3.4678,
        |  "col_D": {
        |     "col_E": 6
        |   },
        |  "col_F": [
        |    {
        |       "col_G": 4.356343
        |    }
        |  ]
        |}
        |""".stripMargin
    )

    val transformedDF = df.castFromToDataTypes(
      dataTypeMapper = Map(
        "long" -> "integer",
        "double" -> "decimal(5, 2)"
      ),
      castRecursively = false
    )

    transformedDF.schema.fields
      .filter(_.name == "col_A")
      .head
      .dataType shouldBe IntegerType

    transformedDF.schema.fields
      .filter(_.name == "col_B")
      .head
      .dataType shouldBe IntegerType

    transformedDF.schema.fields
      .filter(_.name == "col_C")
      .head
      .dataType shouldBe new DecimalType(5, 2)

    transformedDF
      .select("col_D.col_E")
      .schema
      .fields
      .head
      .dataType shouldBe LongType

    (
      transformedDF.schema
        .filter(_.name == "col_F")
        .head
        .dataType match {
        case ArrayType(nestedArrayType: StructType, _) =>
          nestedArrayType
            .filter(_.name == "col_G")
            .head
            .dataType
      }
    ) shouldBe DoubleType
  }

  "castNestedColumn() - with nested column name and schema ddl" should "cast nested column correctly" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": {
        |     "col_C": "val_C",
        |     "col_D": 5
        |  }
        |}
        |""".stripMargin
    )

    val transformedDF = df.castNestedColumn(
      columnName = "col_B",
      schemaDDL = "col_C STRING, col_D STRING"
    )

    transformedDF.schema.fields
      .filter(_.name == "col_B")
      .head
      .toDDL shouldBe "col_B STRUCT<col_C: STRING, col_D: STRING>"
  }

  "changeCaseOfColumnNames() - with 'lower' targetCase" should "renames all the columns to lower case" in {
    val df = readJSON(
      """|{
         |  "col_a": "1",
         |  "COL_B": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      targetCaseType = "lower"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col_a": "1",
        |  "col_b": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "changeCaseOfColumnNames() - with 'kebab' targetCase and 'snake' sourceCase" should "renames all the columns to kebab case" in {
    val df = readJSON(
      """|{
         |  "col_a": "1",
         |  "COL_B": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      sourceCaseType = "snake",
      targetCaseType = "kebab"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col-a": "1",
        |  "col-b": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "changeCaseOfColumnNames() - with 'camel' targetCase and 'snake' sourceCase" should "rename all the columns to camel case" in {
    val df = readJSON(
      """|{
         |  "col_a": "1",
         |  "COL_B": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      sourceCaseType = "snake",
      targetCaseType = "camel"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "colA": "1",
        |  "colB": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "changeCaseOfColumnNames() - with 'pascal' targetCase and 'snake' sourceCase" should "rename all the columns to pascal case" in {
    val df = readJSON(
      """|{
         |  "col_a": "1",
         |  "COL_B": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      sourceCaseType = "snake",
      targetCaseType = "pascal"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "ColA": "1",
        |  "ColB": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "changeCaseOfColumnNames() - with 'snake' targetCase and 'camel' sourceCase" should "rename all the columns to snake case" in {
    val df = readJSON(
      """|{
         |  "colA": "1",
         |  "colB": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      sourceCaseType = "camel",
      targetCaseType = "snake"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col_a": "1",
        |  "col_b": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "changeCaseOfColumnNames() - with 'kebab' targetCase and 'camel' sourceCase" should "rename all the columns to kebab case" in {
    val df = readJSON(
      """|{
         |  "colA": "1",
         |  "colB": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      sourceCaseType = "camel",
      targetCaseType = "kebab"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col-a": "1",
        |  "col-b": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "changeCaseOfColumnNames() - with 'pascal' targetCase and 'camel' sourceCase" should "rename all the columns to pascal case" in {
    val df = readJSON(
      """|{
         |  "colA": "1",
         |  "colB": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      sourceCaseType = "camel",
      targetCaseType = "pascal"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "ColA": "1",
        |  "ColB": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "changeCaseOfColumnNames() - with 'snake' targetCase and 'pascal' sourceCase" should "rename all the columns to snake case" in {
    val df = readJSON(
      """|{
         |  "ColA": "1",
         |  "ColB": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      sourceCaseType = "pascal",
      targetCaseType = "snake"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col_a": "1",
        |  "col_b": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "changeCaseOfColumnNames() - with 'kebab' targetCase and 'pascal' sourceCase" should "rename all the columns to kebab case" in {
    val df = readJSON(
      """|{
         |  "ColA": "1",
         |  "ColB": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      sourceCaseType = "pascal",
      targetCaseType = "kebab"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "col-a": "1",
        |  "col-b": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "changeCaseOfColumnNames() - with 'camel' targetCase and 'pascal' sourceCase" should "rename all the columns to camel case" in {
    val df = readJSON(
      """|{
         |  "ColA": "1",
         |  "ColB": "2"
         |}""".stripMargin
    )

    val actualDF = df.changeCaseOfColumnNames(
      sourceCaseType = "pascal",
      targetCaseType = "camel"
    )

    val expectedDF = readJSON(
      """
        |{
        |  "colA": "1",
        |  "colB": "2"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "flattenSchema()" should "flatten the dataframe" in {
    val df = readJSON(
      """
        |{
        |  "rewardApprovedMonthPeriod": {
        |      "from": "2021-09",
        |      "to": "2021-10"
        |   }
        |}
        |""".stripMargin
    )

    val actualDF = df.flattenSchema

    val expectedDF = readJSON(
      """
        |{
        |  "rewardApprovedMonthPeriod_from": "2021-09",
        |  "rewardApprovedMonthPeriod_to": "2021-10"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "renameColumns() - with column name mapper" should "rename all specified columns" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "val_A",
        |  "col_B": "val_B",
        |  "col_C": "val_C"
        |}
        |""".stripMargin
    )

    val actualDF = df.renameColumns(renameColumnMapper =
      Map(
        "col_A" -> "A",
        "col_B" -> "B",
        "col_C" -> "C"
      )
    )

    val expectedDF = readJSON(
      """
        |{
        |  "A": "val_A",
        |  "B": "val_B",
        |  "C": "val_C"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "replaceEmptyStringsWithNulls()" should "replace all empty strings with nulls" in {
    val df = readJSON(
      """
        |{
        |  "col_A": "",
        |  "col_B": "val_B",
        |  "col_C": ""
        |}
        |""".stripMargin
    )

    val actualDF = df.replaceEmptyStringsWithNulls

    val expectedDF = readJSON(
      """
        |{
        |  "col_A": null,
        |  "col_B": "val_B",
        |  "col_C": null
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "splitColumn()" should "split the column and create new columns accordingly" in {
    val df = readJSON(
      """
        |{
        | "address": "Apt-123,XYZ Building,Pune,Maharashtra"
        |}
        |""".stripMargin
    )

    val actualDF = df.splitColumn(
      fromColumn = "address",
      delimiter = ",",
      toColumns = Map(
        "apt_number" -> 0,
        "society_name" -> 1,
        "city" -> 2,
        "state" -> 3
      )
    )

    val expectedDF = readJSON(
      """
        |{
        | "address": "Apt-123,XYZ Building,Pune,Maharashtra",
        | "apt_number": "Apt-123",
        | "society_name": "XYZ Building",
        | "city": "Pune",
        | "state": "Maharashtra"
        |}
        |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

}
