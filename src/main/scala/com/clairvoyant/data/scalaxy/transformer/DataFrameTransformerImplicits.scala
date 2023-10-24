package com.clairvoyant.data.scalaxy.transformer

import com.clairvoyant.data.scalaxy.transformer.DataFrameTransformerHelper.*
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.{Column, DataFrame}

object DataFrameTransformerImplicits {

  extension (df: DataFrame) {

    // --- PRIVATE METHODS --- //

    private def addColumn(
        dataFrame: DataFrame,
        columnName: String,
        columnValue: String,
        columnDataType: Option[String]
    ): DataFrame =
      columnDataType
        .map(dataType => dataFrame.withColumn(columnName, lit(columnValue).cast(dataType)))
        .getOrElse(dataFrame.withColumn(columnName, lit(columnValue)))

    private def addColumnWithExpression(
        dataFrame: DataFrame,
        columnName: String,
        columnExpression: String,
        columnDataType: Option[String]
    ): DataFrame =
      columnDataType
        .map(dataType => dataFrame.withColumn(columnName, expr(columnExpression).cast(dataType)))
        .getOrElse(dataFrame.withColumn(columnName, expr(columnExpression)))

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

    private def applyChangeNameFunctionRecursively(
        schema: StructType,
        changeNameFunction: String => String
    ): StructType =
      StructType {
        schema.flatMap {
          case sf @ StructField(
                name,
                _ @ArrayType(arrayNestedType: StructType, containsNull),
                nullable,
                metadata
              ) =>
            StructType {
              Seq(
                sf.copy(
                  changeNameFunction(name),
                  ArrayType(
                    applyChangeNameFunctionRecursively(arrayNestedType, changeNameFunction),
                    containsNull
                  ),
                  nullable,
                  metadata
                )
              )
            }

          case sf @ StructField(
                name,
                structType: StructType,
                nullable,
                metadata
              ) =>
            StructType {
              Seq(
                sf.copy(
                  changeNameFunction(name),
                  applyChangeNameFunctionRecursively(structType, changeNameFunction),
                  nullable,
                  metadata
                )
              )
            }

          case sf @ StructField(
                name,
                _,
                _,
                _
              ) =>
            StructType {
              Seq(sf.copy(name = changeNameFunction(name)))
            }
        }
      }

    // --- PUBLIC METHODS --- //

    /**
     * Adds a new column with a literal value of the desired data type
     * @param columnName
     *   Name of the new column to be added
     * @param columnValue
     *   Literal value of the new column
     * @param columnDataType
     *   The spark sql data type that new column needs to be casted into
     * @param replaceExisting
     *   If set to true, if a column already exists with the same name as `columnName`, it will get replaced with the
     *   new value. If set to false, then it returns original dataframe.
     * @return
     *   DataFrame with the new column added
     */
    def addColumn(
        columnName: String,
        columnValue: String,
        columnDataType: Option[String] = None,
        replaceExisting: Boolean = false
    ): DataFrame =
      if (df.columns.exists(_.toLowerCase == columnName.toLowerCase)) {
        if (replaceExisting)
          addColumn(df.drop(columnName), columnName, columnValue, columnDataType)
        else
          df
      } else
        addColumn(df, columnName, columnValue, columnDataType)

    /**
     * Adds a new column with an expression value of the desired data type
     *
     * @param columnName
     *   Name of the new column to be added
     * @param columnExpression
     *   Expression for the value of the new column
     * @param columnDataType
     *   The spark sql data type that new column needs to be casted into
     * @param replaceExisting
     *   If set to true, if a column already exists with the same name as `columnName`, it will get replaced with the
     *   new value. If set to false, then it returns original dataframe.
     * @return
     *   DataFrame with the new column added
     */
    def addColumnWithExpression(
        columnName: String,
        columnExpression: String,
        columnDataType: Option[String] = None,
        replaceExisting: Boolean = false
    ): DataFrame =
      if (df.columns.exists(_.toLowerCase == columnName.toLowerCase)) {
        if (replaceExisting)
          addColumnWithExpression(df.drop(columnName), columnName, columnExpression, columnDataType)
        else
          df
      } else
        addColumnWithExpression(df, columnName, columnExpression, columnDataType)

    /**
     * Adds a desired prefix to column names
     * @param prefix
     *   The desired prefix to be added to column names
     * @param columnNames
     *   The list of column names to which the prefix should be added. If the list is empty, then prefix gets added to
     *   all column names.
     * @return
     *   DataFrame with prefix added to column names
     */
    def addPrefixToColumnNames(
        prefix: String,
        columnNames: List[String] = List.empty
    ): DataFrame =
      addPrefixOrSuffixToColumnNames(
        prefixOrSuffixFunction = (columnName: String) => s"${prefix}_$columnName",
        columnNames = columnNames
      )

    /**
     * Adds a desired suffix to column names
     *
     * @param suffix
     *   The desired suffix to be added to column names
     * @param columnNames
     *   The list of column names to which the suffix should be added. If the list is empty, then suffix gets added to
     *   all column names.
     * @return
     *   DataFrame with suffix added to column names
     */
    def addSuffixToColumnNames(
        suffix: String,
        columnNames: List[String] = List.empty
    ): DataFrame =
      addPrefixOrSuffixToColumnNames(
        prefixOrSuffixFunction = (columnName: String) => s"${columnName}_$suffix",
        columnNames = columnNames
      )

    /**
     * Casts the data type of multiple columns to the desired different types at once
     * @param columnDataTypeMapper
     *   Mapping of column names to its corresponding desired data types
     * @return
     *   DataFrame with columns casted to data types as specified in the mapper
     */
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

    /**
     * Casts the data type of multiple columns to the desired different types at once based on the prefix of the columns
     * @param prefix
     *   Prefix string based on which given columns to be selected to cast them to the desired data type
     * @param dataType
     *   The desired data type to which the columns have to be casted
     * @return
     *   DataFrame with columns casted to new data type based on the specified prefix for column names
     */
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

    /**
     * Casts the data type of multiple columns to the desired different types at once based on the suffix of the columns
     *
     * @param suffix
     *   Suffix string based on which given columns to be selected to cast them to the desired data type
     * @param dataType
     *   The desired data type to which the columns have to be casted
     * @return
     *   DataFrame with columns casted to new data type based on the specified suffix for column names
     */
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

    /**
     * Casts all columns having X data type to a different Y data type
     *
     * @param dataTypeMapper
     *   Defines the mapping of source data type and target data type
     * @param castRecursively
     *   Flag that tells whether casting needs to be performed at nested level
     * @return
     *   DataFrame with casting of columns done as specified in the data type mapper
     */
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

    /**
     * Casts the data type of any nested or struct type column from one type to another
     * @param columnName
     *   The name of the nested column
     * @param schemaDDL
     *   The new Data Definition Language (DDL) for the column
     * @return
     *   DataFrame with the nested column casted to specified type as per DDL
     */
    def castNestedColumn(
        columnName: String,
        schemaDDL: String
    ): DataFrame = df.withColumn(columnName, from_json(to_json(col(columnName)), DataType.fromDDL(schemaDDL)))

    /**
     * Changes the case of column names
     * @param sourceCaseType
     *   The original case type
     * @param targetCaseType
     *   The required case type
     * @return
     *   DataFrame with the changed case of columns
     */
    def changeCaseOfColumnNames(
        sourceCaseType: String = "lower",
        targetCaseType: String
    ): DataFrame = {
      val converter =
        targetCaseType.toLowerCase() match {
          case "camel" =>
            new CamelCaseConverter()
          case "snake" =>
            new SnakeCaseConverter()
          case "pascal" =>
            new PascalCaseConverter()
          case "kebab" =>
            new KebabCaseConverter()
          case "lower" =>
            new LowerCaseConverter()
          case "upper" =>
            new UpperCaseConverter()
          case _ =>
            throw new Exception(s"The provided caseType: $targetCaseType is not supported.")
        }

      val renamedColumnNames: Seq[String] = df.columns.map { columnName =>
        converter.convert(columnName, sourceCaseType)
      }

      df.select(df.columns.zip(renamedColumnNames).map { case (original, renamed) =>
        col(original).as(renamed)
      }: _*)
    }

    /**
     * Converts the columns of array of struct type to array of json string type
     *
     * @return
     *   DataFrame with the columns of array of struct type converted to array of json string type
     */
    def convertArrayOfStructToArrayOfJSONString: DataFrame =
      df.schema.fields
        .filter(_.dataType.sql.toLowerCase().startsWith("array<struct"))
        .map(_.name)
        .foldLeft(df) { (dataFrame, fieldName) =>
          dataFrame.withColumn(fieldName, transform(col(fieldName), column => to_json(column)))
        }

    /**
     * Converts the column with JSON string as value to struct type
     *
     * @param columnName
     *   Name of the column to be converted
     * @return
     *   DataFrame with the column converted to struct type
     */
    def convertJSONStringToStruct(
        columnName: String
    ): DataFrame =
      import df.sparkSession.implicits.*
      df.withColumn(
        columnName,
        from_json(
          col(columnName),
          df.sparkSession.read.json(df.select(columnName).as[String]).schema
        )
      )

    /**
     * Flattens the schema of the dataframe. If any of the column is of StructType or is nested, this transformation
     * removes the nested structure and represent each nested attribute at a root level.
     * @return
     *   DataFrame with the flattened schema
     */
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

    /**
     * Renames one or multiple columns at once
     * @param renameColumnMapper
     *   Defines the mapping of the existing and desired column name
     * @return
     *   DataFrame with columns renamed as specified in the mapper
     */
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

    /**
     * Replaces a specific text in column name with the another text
     * @param columnName
     *   The column name to be modified
     * @param pattern
     *   The sequence of characters or text to be replaced
     * @param replacement
     *   The target text to replace with
     * @param replaceRecursively
     *   Flag to determine if operation needs to be performed at root level only or at nested level
     * @return
     *   A dataframe with the column name modified
     */
    def replaceStringInColumnName(
        columnName: String,
        pattern: String,
        replacement: String,
        replaceRecursively: Boolean
    ): DataFrame =
      val replaceStringInColumnNameFunction =
        (colName: String) =>
          if (colName == columnName)
            colName.replace(pattern, replacement)
          else
            colName

      if (replaceRecursively)
        df.sparkSession.createDataFrame(
          rowRDD = df.rdd,
          schema = applyChangeNameFunctionRecursively(
            schema = df.schema,
            changeNameFunction = replaceStringInColumnNameFunction
          )
        )
      else
        df.withColumnRenamed(columnName, replaceStringInColumnNameFunction(columnName))

    /**
     * Creates new columns using the value of another column that is a delimiter separated string.
     * @param fromColumn
     *   Name of the source column having delimiter separated string as a value from which new columns need to be
     *   created
     * @param delimiter
     *   The delimiter by which a string is separated
     * @param toColumns
     *   It is a map of new column name against the position of the value that is needed from the delimiter separated
     *   string
     * @return
     *   DataFrame with new columns being created using the mapping specified in the `toColumns`
     */
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
