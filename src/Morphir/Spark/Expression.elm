module Morphir.Spark.Expression exposing (..)

import Morphir.IR.Literal exposing (Literal(..))
{-| An Expression represents an column expression.
Expressions produce a value that is usually of type `Column` in spark. ,
An Expression could take in a `Column` type or `Any` type as input and also produce a Column type and for this reason,
Expression is expressed as a recursive type.

These are the supported Expressions:

  - **Column**
      - Specifies the name of a column in a DataFrame similar to the `col("name")` in spark
  - **Literal**
      - Represents a literal value like `1`, `"Hello"`, `2.3`.
  - **Variable**
      - Represents a variable name like `param`.
  - **BinaryOperation**
      - BinaryOperations represent binary operations like `1 + 2`.
      - The three arguments are: the operator, the left hand side expression, and the right hand side expression
  - **WhenOtherwise**
      - Represent a `when(expression, result).otherwise(expression, result)` in spark.
      - It maps directly to an IfElse statement and can be chained.
      - The three arguments are: the condition, the Then expression evaluated if the condition passes, and the Else expression.
  - **Method**
      - Applies a list of arguments on a method to a target instance.
      - The three arguments are: An expression denoting the target instance, the name of the method to invoke, and a list of arguments to invoke the method with
  - **Function**
      - Applies a list of arguments on a function.
      - The two arguments are: The fully qualified name of the function to invoke, and a list of arguments to invoke the function with

-}
type Expression
    = Column String
    | Literal Literal
    | Variable String
    | BinaryOperation String Expression Expression
    | WhenOtherwise Expression Expression Expression
    | Method Expression String (List Expression)
    | Function String (List Expression)