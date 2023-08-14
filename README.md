# data-scalaxy-transformer

This library provides users with multiple transformation APIs that can be triggered on a spark dataframe object.

## List of transformation APIs

User can use below available API methods that can be called on a spark dataframe:

* addColumn
* addColumnWithExpression
* addPrefixToColumnNames
* addSuffixToColumnNames
* castColumns
* castColumnsBasedOnPrefix
* castColumnsBasedOnSuffix
* castFromToDataTypes
* castNestedColumn
* changeCaseOfColumnNames
* flattenSchema
* renameColumns
* replaceEmptyStringsWithNulls
* splitColumn

## Documentation

You can find the documentation for all the above APIs
[here](src%2Fmain%2Fscala%2Fcom%2Fclairvoyant%2Fdata%2Fscalaxy%2Ftransformer%2FDataFrameTransformerImplicits.scala)

## Usage

You can find the usage examples of all above APIs
[here](src%2Ftest%2Fscala%2Fcom%2Fclairvoyant%2Fdata%2Fscalaxy%2Ftransformer%2FDataFrameTransformerImplicitsSpec.scala)