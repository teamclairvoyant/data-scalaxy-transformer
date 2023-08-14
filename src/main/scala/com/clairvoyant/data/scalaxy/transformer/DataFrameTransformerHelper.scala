package com.clairvoyant.data.scalaxy.transformer

object DataFrameTransformerHelper {

  trait CaseConverter {
    def convert(inputString: String, sourceCaseType: String): String
  }

  class CamelCaseConverter extends CaseConverter {

    def convert(inputString: String, sourceCaseType: String): String =
      sourceCaseType.toLowerCase() match {
        case "snake" =>
          snakeToCamel(inputString)
        case "pascal" =>
          pascalToCamel(inputString)
        case "kebab" =>
          kebabToCamel(inputString)
        case _ =>
          throw new Exception("Camel-case conversion only supported for source case types : Snake/Pascal/Kebab..")
      }

    private def snakeToCamel(snakeCase: String): String = {
      val words = snakeCase.split("_")
      words.headOption.getOrElse("").toLowerCase + words.tail.map(_.capitalize).mkString
    }

    private def pascalToCamel(pascalCase: String): String = pascalCase.head.toLower + pascalCase.tail

    private def kebabToCamel(kebabCase: String): String = {
      val words = kebabCase.split("-")
      words.headOption.getOrElse("").toLowerCase + words.tail.map(_.capitalize).mkString
    }

  }

  class KebabCaseConverter extends CaseConverter {

    def convert(inputString: String, sourceCaseType: String): String =
      sourceCaseType.toLowerCase() match {
        case "snake" =>
          snakeToKebab(inputString)
        case "camel" =>
          camelToKebab(inputString)
        case "pascal" =>
          pascalToKebab(inputString)
        case _ =>
          throw new Exception("Kebab-case conversion only supported for source case types : Snake/Camel/Pascal..")
      }

    private def snakeToKebab(snakeCase: String): String = snakeCase.replaceAll("_", "-").toLowerCase()

    private def camelToKebab(camelCase: String): String =
      "([a-z])([A-Z]+)".r.replaceAllIn(camelCase, "$1-$2").toLowerCase()

    private def pascalToKebab(pascalCase: String): String =
      "[A-Z\\d]".r.replaceAllIn(pascalCase, { m => "-" + m.group(0).toLowerCase() }).stripPrefix("-")

  }

  class LowerCaseConverter extends CaseConverter {

    def convert(inputString: String, sourceCaseType: String = "upper"): String = inputString.toLowerCase()

  }

  class PascalCaseConverter extends CaseConverter {

    def convert(inputString: String, sourceCaseType: String): String =
      sourceCaseType.toLowerCase() match {
        case "snake" =>
          snakeToPascal(inputString)
        case "camel" =>
          camelToPascal(inputString)
        case "kebab" =>
          kebabToPascal(inputString)
        case _ =>
          throw new Exception("Pascal-case conversion only supported for source case types : Snake/Camel/Kebab..")
      }

    private def snakeToPascal(snakeCase: String): String = snakeCase.toLowerCase.split("_").map(_.capitalize).mkString

    private def camelToPascal(camelCase: String): String = camelCase.head.toUpper + camelCase.tail

    private def kebabToPascal(kebabCase: String): String = kebabCase.toLowerCase.split("-").map(_.capitalize).mkString

  }

  class SnakeCaseConverter extends CaseConverter {

    def convert(inputString: String, sourceCaseType: String): String =
      sourceCaseType.toLowerCase() match {
        case "camel" =>
          camelToSnake(inputString)
        case "pascal" =>
          pascalToSnake(inputString)
        case "kebab" =>
          kebabToSnake(inputString)
        case _ =>
          throw new Exception("Snake-case conversion only supported for source case types : Camel/Pascal/Kebab..")
      }

    private def camelToSnake(camelCase: String): String =
      "([a-z])([A-Z]+)".r.replaceAllIn(camelCase, "$1_$2").toLowerCase()

    private def pascalToSnake(pascalCase: String): String =
      "[A-Z\\d]".r.replaceAllIn(pascalCase, { m => "_" + m.group(0).toLowerCase() }).stripPrefix("_")

    private def kebabToSnake(kebabCase: String): String = kebabCase.replaceAll("-", "_").toLowerCase()

  }

  class UpperCaseConverter extends CaseConverter {

    def convert(inputString: String, sourceCaseType: String = "lower"): String = inputString.toUpperCase()

  }

}
