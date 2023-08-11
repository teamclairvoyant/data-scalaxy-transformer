package com.clairvoyant.data.scalaxy.transformer

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object DataFrameTransformerImplicits {

  extension (df: DataFrame) {

    def addColumn(
        columnName: String,
        columnValue: String,
        columnDataType: Option[String]
    ): DataFrame =
      columnDataType
        .map(dataType => df.withColumn(columnName, lit(columnValue).cast(dataType)))
        .getOrElse(df.withColumn(columnName, lit(columnValue)))

    def addColumnWithExpression(
        columnName: String,
        columnValueExpression: String,
        columnDataType: Option[String]
    ): DataFrame =
      columnDataType
        .map(dataType => df.withColumn(columnName, expr(columnValueExpression).cast(dataType)))
        .getOrElse(df.withColumn(columnName, expr(columnValueExpression)))

    private def addPrefixOrSuffixToColumnNames(
        prefixOrSuffixFunction: String => String,
        columnNames: List[String] = List.empty
    ): DataFrame =
      if (columnNames.isEmpty)
        df.renameColumns(
          df.columns
            .map(columnName => columnName -> prefixOrSuffixFunction(columnName))
            .toMap
        )
      else
        df.renameColumns(
          df.columns.map { columnName =>
            if (columnNames.contains(columnName))
              columnName -> prefixOrSuffixFunction(columnName)
            else
              columnName -> columnName
          }.toMap
        )

    def addPrefixToColumnNames(
        prefix: String,
        columnNames: List[String] = List.empty
    ): DataFrame =
      addPrefixOrSuffixToColumnNames(
        prefixOrSuffixFunction = (columnName: String) => s"${prefix}_$columnName",
        columnNames = columnNames
      )

    def addSuffixToColumnNames(
        suffix: String,
        columnNames: List[String] = List[String]()
    ): DataFrame =
      addPrefixOrSuffixToColumnNames(
        prefixOrSuffixFunction = (columnName: String) => s"${columnName}_$suffix",
        columnNames = columnNames
      )

    def castColumns(columnDataTypeMapper: Map[String, String]): DataFrame = {
      val timestampDataTypeRegexPattern = "timestamp(?:\\((.*)\\))?".r
      val dateDataTypeRegexPattern = "date(?:\\((.*)\\))?".r

      df.select(
        df.columns
          .map { columnName =>
            columnDataTypeMapper
              .get(columnName)
              .map {
                case timestampDataTypeRegexPattern(timestampFormat) =>
                  {
                    Option(timestampFormat) match {
                      case Some(timestampFormat) =>
                        to_timestamp(col(columnName), timestampFormat)
                      case None =>
                        to_timestamp(col(columnName))
                    }
                  }.as(columnName)
                case dateDataTypeRegexPattern(dateFormat) =>
                  {
                    Option(dateFormat) match {
                      case Some(dateFormat) =>
                        to_date(col(columnName), dateFormat)
                      case None =>
                        to_date(col(columnName))
                    }
                  }.as(columnName)
                case dataType =>
                  col(columnName).cast(dataType)
              }
              .getOrElse(col(columnName))
          }*
      )
    }

    def castColumnsBasedOnPrefix(
        prefix: String,
        dataTypeToCast: String
    ): DataFrame =
      castColumns(
        df.columns
          .filter(_.startsWith(prefix))
          .map(_ -> dataTypeToCast)
          .toMap
      )

    def castColumnsBasedOnSuffix(
        suffix: String,
        dataTypeToCast: String
    ): DataFrame =
      castColumns(
        df.columns
          .filter(_.endsWith(suffix))
          .map(_ -> dataTypeToCast)
          .toMap
      )

    def castFromToDataTypes(
        dataTypeMapper: Map[String, String],
        castRecursively: Boolean
    ): DataFrame =
      dataTypeMapper.foldLeft(df) { (dataFrame, dataTypesPair) =>
        val fromDataType = CatalystSqlParser.parseDataType(dataTypesPair._1)
        val toDataType = CatalystSqlParser.parseDataType(dataTypesPair._2)

        if (castRecursively) {
          def applyCastFunctionRecursively(
              schema: StructType,
              fromDataType: DataType,
              toDataType: DataType
          ): StructType =
            StructType(
              schema.flatMap {
                case sf @ StructField(_, ArrayType(arrayNestedType: StructType, containsNull), _, _) =>
                  StructType(
                    Seq(
                      sf.copy(
                        dataType = ArrayType(
                          applyCastFunctionRecursively(arrayNestedType, fromDataType, toDataType),
                          containsNull
                        )
                      )
                    )
                  )

                case sf @ StructField(_, structType: StructType, _, _) =>
                  StructType(
                    Seq(
                      sf.copy(
                        dataType = applyCastFunctionRecursively(structType, fromDataType, toDataType)
                      )
                    )
                  )

                case sf @ StructField(_, dataType: DataType, _, _) =>
                  StructType(
                    Seq(
                      if (dataType == fromDataType)
                        sf.copy(dataType = toDataType)
                      else
                        sf
                    )
                  )
              }
            )

          val newSchema = applyCastFunctionRecursively(dataFrame.schema, fromDataType, toDataType)
          dataFrame.sparkSession.read.schema(newSchema).json(dataFrame.toJSON)
        } else
          dataFrame.select(
            dataFrame.schema.map { structField =>
              if (structField.dataType == fromDataType)
                col(structField.name).cast(toDataType)
              else
                col(structField.name)
            }.toList*
          )
      }

    def castNestedColumn(
        columnName: String,
        ddl: String
    ): DataFrame = df.withColumn(columnName, from_json(to_json(col(columnName)), DataType.fromDDL(ddl)))

    private def applyChangeNameFunctionRecursively(
        schema: StructType,
        changeNameFunction: String => String
    ): StructType =
      StructType(
        schema.flatMap {
          case sf @ StructField(
                name,
                ArrayType(arrayNestedType: StructType, containsNull),
                nullable,
                metadata
              ) =>
            StructType(
              Seq(
                sf.copy(
                  changeNameFunction(name),
                  dataType = ArrayType(
                    applyChangeNameFunctionRecursively(arrayNestedType, changeNameFunction),
                    containsNull
                  ),
                  nullable,
                  metadata
                )
              )
            )
          case sf @ StructField(name, structType: StructType, nullable, metadata) =>
            StructType(
              Seq(
                sf.copy(
                  changeNameFunction(name),
                  dataType = applyChangeNameFunctionRecursively(structType, changeNameFunction),
                  nullable,
                  metadata
                )
              )
            )

          case sf @ StructField(name, _, _, _) =>
            StructType(
              Seq(
                sf.copy(name = changeNameFunction(name))
              )
            )
        }
      )

    def convertColumnToJson(columnName: String): DataFrame = df.withColumn(columnName, to_json(col(columnName)))

    def deleteColumns(columnNames: List[String]): DataFrame = df.drop(columnNames*)

    def explodeColumn(columnName: String): DataFrame = df.withColumn(columnName, explode(col(columnName)))

    def filterRecords(filterCondition: String): DataFrame = df.filter(filterCondition)

    def flattenSchema: DataFrame = {
      def flattenSchemaFromStructType(
          schema: StructType,
          prefix: Option[String] = None
      ): Array[Column] =
        schema.fields.flatMap { field =>
          val newColName = prefix.map(p => s"$p.${field.name}").getOrElse(field.name)

          field.dataType match {
            case st: StructType =>
              flattenSchemaFromStructType(st, Some(newColName))
            case _ =>
              Array(col(newColName).as(newColName.replace(".", "_")))
          }
        }

      if (df.schema.exists(_.dataType.isInstanceOf[StructType]))
        df.select(flattenSchemaFromStructType(df.schema)*)
      else
        df
    }

    def renameColumns(renameColumnMapper: Map[String, String]): DataFrame =
      df.select(
        df.columns
          .map(columnName =>
            renameColumnMapper
              .get(columnName)
              .map(col(columnName).name)
              .getOrElse(col(columnName))
          )*
      )

    def replaceEmptyStringsWithNulls: DataFrame = df.na.replace(df.columns, Map("" -> null))

    def replaceStringInColumnValue(columnName: String, pattern: String, replacement: String): DataFrame =
      df.withColumn(columnName, regexp_replace(col(columnName), pattern, replacement))

    def selectColumns(columnNames: List[String]): DataFrame = df.select(columnNames.map(col)*)

    def splitColumn(
        fromColumn: String,
        delimiter: String,
        toColumns: Map[String, Int]
    ): DataFrame =
      toColumns.foldLeft(df) { (df, columnNamePositionPair) =>
        df.withColumn(
          columnNamePositionPair._1,
          split(col(fromColumn), delimiter).getItem(columnNamePositionPair._2)
        )
      }

  }

}
