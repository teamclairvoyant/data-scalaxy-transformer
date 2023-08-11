package com.clairvoyant.data.scalaxy.transformer

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.{Column, DataFrame}

object DataFrameTransformerImplicits {

  extension (df: DataFrame) {

    // --- PRIVATE METHODS --- //

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

    private def castColumn(
        columnName: String,
        dataType: String | DataType
    ): Column = {
      val timestampDataTypeRegexPattern = "timestamp(?:\\((.*)\\))?".r
      val dateDataTypeRegexPattern = "date(?:\\((.*)\\))?".r

      dataType match {
        case dt: String =>
          dt
        case dt: DataType =>
          dt.typeName
      } match {
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
    }

    // --- PUBLIC METHODS --- //

    def addColumn(
        columnName: String,
        columnValue: String,
        columnDataType: Option[String] = None
    ): DataFrame =
      columnDataType
        .map(dataType => df.withColumn(columnName, lit(columnValue).cast(dataType)))
        .getOrElse(df.withColumn(columnName, lit(columnValue)))

    def addColumnWithExpression(
        columnName: String,
        columnExpression: String,
        columnDataType: Option[String] = None
    ): DataFrame =
      columnDataType
        .map(dataType => df.withColumn(columnName, expr(columnExpression).cast(dataType)))
        .getOrElse(df.withColumn(columnName, expr(columnExpression)))

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
        columnNames: List[String] = List.empty
    ): DataFrame =
      addPrefixOrSuffixToColumnNames(
        prefixOrSuffixFunction = (columnName: String) => s"${columnName}_$suffix",
        columnNames = columnNames
      )

    def castColumns(columnDataTypeMapper: Map[String, String]): DataFrame =
      df.select(
        df.columns
          .map { columnName =>
            columnDataTypeMapper
              .get(columnName)
              .map(castColumn(columnName, _))
              .getOrElse(col(columnName))
          }*
      )

    def castColumnsBasedOnPrefix(
        prefix: String,
        dataType: String
    ): DataFrame =
      castColumns(
        df.columns
          .filter(_.startsWith(prefix))
          .map(_ -> dataType)
          .toMap
      )

    def castColumnsBasedOnSuffix(
        suffix: String,
        dataType: String
    ): DataFrame =
      castColumns(
        df.columns
          .filter(_.endsWith(suffix))
          .map(_ -> dataType)
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
                castColumn(
                  columnName = structField.name,
                  dataType = toDataType
                )
              else
                col(structField.name)
            }.toList*
          )
      }

    def castNestedColumn(
        columnName: String,
        schemaDDL: String
    ): DataFrame = df.withColumn(columnName, from_json(to_json(col(columnName)), DataType.fromDDL(schemaDDL)))

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
