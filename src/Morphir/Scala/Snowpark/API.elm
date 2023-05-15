-- File that contains all the functions to generate snowpark in scala

module Morphir.Scala.Snowpark.API exposing (..)

import Morphir.Scala.AST as Scala
import Morphir.Spark.API as API
import Morphir.IR.Value exposing (literal)
import Morphir.Spark.Expression exposing (Expression(..))

-- Define the namespace for Snowpark as a List of String
snowparkNamespace : List String
snowparkNamespace = [ "com", "snowflake", "snowpark" ]

-- Define the namespace for the functions defined in Snowpark
snowparkFunctionsNamespace : List String
snowparkFunctionsNamespace = snowparkNamespace ++ ["functions"]

-- Funtion to generate a literal with the snowflake namespace
literalCreation : Scala.Value -> Scala.Value
literalCreation lit =
        Scala.Apply (Scala.Ref snowparkFunctionsNamespace "lit")
            [ Scala.ArgValue Nothing lit ]
            
{--
 Function to wrap a Scala.Literal as a Literal (lit)
 Any other value will be return in the same way
 Example: True is converted as lit(True)
--}
wrapLiteral : Scala.Value -> Scala.Value
wrapLiteral value =
        case value of
             Scala.Literal x  ->
                literalCreation (Scala.Literal x)
             _ ->
                value

-- This Record contains all the function definition used it to generate snowpark
snowparkApi : API.DataFrameApi
snowparkApi = {

    -- This function is used to generate the in function and receives obj (Expression) and args(Expression) to generate the arguments as a Sequence (Seq)
    isInMethod = \obj args ->
        Method obj "in" [Function "Seq" args]

    -- This function is used to generate the is_null function and receives args (List Expression) to generate a is_null(args) code.
    , isNullFunction = \args ->
        Function "is_null" args

    {-- 
    This function is used to generate the when function and receives args (Expression). 
    it validates if args is a literal, in that case generate lit(args)
    otherwise generates args
    --}
    , whenExpression = \args ->
        Function "when" (case args of
                            [condition, Literal _ as value] ->
                                [condition, Function "lit" [value]]
                            _ ->
                                args)

    -- Contains the string to the functionNamespace  in snowpark
    , functionsNamespace = snowparkFunctionsNamespace

    -- This function generates a DataFrame object with the snowpark namespace 
    ,dataFrame =
        Scala.TypeRef snowparkNamespace "DataFrame"
    
    -- Function used it to generate a literal (lit). See literalCreation function
    , literal = literalCreation

    -- This function receives a name and generate a call to col method from snowpark with a specify column name
    , column = \name ->
        Scala.Apply (Scala.Ref snowparkFunctionsNamespace "col")
            [ Scala.ArgValue Nothing (Scala.Literal (Scala.StringLit name)) ]


    -- Generate the When structure and wrap the thenBranch into a literal if the argument for the thenBranch is a literal (lit)
    , when = \cond thenBranch ->
             Scala.Apply (Scala.Ref snowparkFunctionsNamespace "when")
            [ Scala.ArgValue Nothing cond, Scala.ArgValue Nothing (wrapLiteral thenBranch) ]

    -- Generate the When structure with soFar and wrap the thenBranch into a literal if the argument for the thenBranch is a literal (lit)
    , andWhen = \cond thenBranch soFar ->
        Scala.Apply
            (Scala.Select soFar "when")
            [ Scala.ArgValue Nothing cond, Scala.ArgValue Nothing (wrapLiteral thenBranch) ]

    -- Generate the elseBranch from a condition, if the elseBranch is a literal it will be wrapper into a lit function
    , otherwise = \elseBranch soFar ->
        Scala.Apply
            (Scala.Select soFar "otherwise")
            [ Scala.ArgValue Nothing  (wrapLiteral elseBranch) ]

    -- Generate the alias as a literal
    , alias = \name value ->
        Scala.Apply
            (Scala.Select value "alias")
            [ Scala.ArgValue Nothing (Scala.Literal (Scala.StringLit name)) ]

    -- This function generates a select from 
    , select = \columns from ->
        Scala.Apply
            (Scala.Select
                from
                "select"
            )
            (columns
                |> List.map (Scala.ArgValue Nothing)
            )

    -- Function to generate a filter from a collection of data
    , filter = \predicate from ->
        Scala.Apply
            (Scala.Select
                from
                "filter"
            )
            [ Scala.ArgValue Nothing predicate
            ]

    -- Function to generate a join function
    , join = \rightRelation predicate joinTypeLabel leftRelation ->
        Scala.Apply
            (Scala.Select
                leftRelation
                "join"
            )
            [ Scala.ArgValue Nothing rightRelation
            , Scala.ArgValue Nothing predicate
            , Scala.ArgValue Nothing
                (Scala.Literal
                    (Scala.StringLit joinTypeLabel)
                )
            ]

    -- Function to generate an aggregate function
    , aggregate = \groupField aggregations from ->
        Scala.Apply
            (Scala.Select
                (Scala.Apply
                    (Scala.Select from "groupBy")
                    [ Scala.ArgValue Nothing (Scala.Literal (Scala.StringLit groupField)) ]
                )
                "agg"
            )
            (aggregations
                |> List.map (Scala.ArgValue Nothing)
            )
 }