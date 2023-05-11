module Morphir.Scala.Snowpark.Backend exposing (..)

import Dict exposing (Dict)
import Morphir.File.FileMap exposing (FileMap)
import Morphir.IR as IR exposing (IR)
import Morphir.IR.AccessControlled as AccessControlled exposing (Access(..), AccessControlled)
import Morphir.IR.Distribution as Distribution exposing (Distribution)
import Morphir.IR.Documented as Documented exposing (Documented)
import Morphir.IR.FQName as FQName exposing (FQName)
import Morphir.IR.Literal exposing (Literal(..))
import Morphir.IR.Module as Module exposing (ModuleName)
import Morphir.IR.Name as Name exposing (Name)
import Morphir.IR.Type as Type exposing (Type)
import Morphir.IR.Value as Value exposing (TypedValue, Value)
import Morphir.SDK.ResultList as ResultList
import Morphir.Scala.AST as Scala
import Morphir.Scala.Common as ScalaBackend
import Morphir.Scala.Feature.Core as ScalaBackend
import Morphir.Scala.PrettyPrinter as PrettyPrinter
import Morphir.Spark.API as Spark
import Morphir.Spark.AST as SparkAST exposing (..)
import Morphir.Spark.Backend as SparkBackend
import Morphir.Scala.Snowpark.API as Snowpark

type alias Options =
    {}

mapDistribution : Options -> Distribution -> FileMap
mapDistribution _ distro =
    let
        fixedDistro =
            SparkBackend.fixDistribution distro
    in
    case fixedDistro of
        Distribution.Library packageName _ packageDef ->
            let
                ir : IR
                ir =
                    IR.fromDistribution fixedDistro
            in
            packageDef.modules
                |> Dict.toList
                |> List.map
                    (\( moduleName, accessControlledModuleDef ) ->
                        let
                            packagePath : List String
                            packagePath =
                                packageName
                                    ++ moduleName
                                    |> List.map (Name.toCamelCase >> String.toLower)

                            object : Scala.TypeDecl
                            object =
                                Scala.Object
                                    { modifiers = []
                                    , name = "SnowparkJobs"
                                    , extends = []
                                    , members =
                                        accessControlledModuleDef.value.values
                                            |> Dict.toList
                                            |> List.filterMap
                                                (\( valueName, _ ) ->
                                                    case SparkBackend.mapFunctionDefinition Snowpark.snowparkApi ir ( packageName, moduleName, valueName ) of
                                                        Ok memberDecl ->
                                                            Just (Scala.withoutAnnotation memberDecl)

                                                        Err err ->
                                                            let
                                                                _ =
                                                                    Debug.log "mapFunctionDefinition error" err
                                                            in
                                                            Nothing
                                                )
                                    , body = Nothing
                                    }

                            compilationUnit : Scala.CompilationUnit
                            compilationUnit =
                                { dirPath = packagePath
                                , fileName = "SnowparkJobs.scala"
                                , packageDecl = packagePath
                                , imports = []
                                , typeDecls = [ Scala.Documented Nothing (Scala.withoutAnnotation object) ]
                                }
                        in
                        ( ( packagePath, "SnowparkJobs.scala" )
                        , PrettyPrinter.mapCompilationUnit (PrettyPrinter.Options 2 80) compilationUnit
                        )
                    )
                |> Dict.fromList