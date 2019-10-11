# Kotlin-JDBC

[![Build Status](https://travis-ci.org/vsch/kotlin-jdbc.svg)](https://travis-ci.org/vsch/kotlin-jdbc)
[![Maven Central status](https://img.shields.io/maven-central/v/com.vladsch.kotlin-jdbc/kotlin-jdbc.svg)](https://search.maven.org/search?q=g:com.vladsch.kotlin-jdbc)<!-- @IGNORE PREVIOUS: link -->

## Version 0.5 API Breaking Release

:information_source: This branch is a rework with breaking changes to refactor models and
related classes to eliminate having to specify identifier quoting in the model by creating the
model for a database session. Which makes sense for a database model class.

Biggest change is that the model now takes two template arguments: main model class and its
associated data class with an optional session instance and identifier quoting string.

If `session` is not given or `null` then default session will be used.

If `quote` if not given or `null` then the connection `metaData.identifierQuoteString` will be
used, anything else will use whatever string is passed in. Unless your jdbc driver does not
provide identifier quoting, there is no need to use anything but the default.

Companion object now only has the table name constant string.

All other functions implemented in the `Model` with two abstract members: `toData()` returning
the data class for the model and `operator invoke` for the factory function for the model. To
get another instance of a model `myModel` invoke the model instance as a function `myModel()`.

For models that do not need a data class `ModelNoData` is also available which only takes a
single template argument, as was the case for the `Model` class in previous releases.

Having the session instance information in the model simplifies using models because session no
longer has to be specified for every method performing database access.

Additionally, list results are simplified because neither the session nor the extractor needs to
be passed, with `myModel.listData()` variations can be used or `myModel.listModel()` variations.

Additionally there is now an `alias:String? = null` argument available for sql generating
functions which will add a table alias to the table name and use the alias for disambiguating
column names. If generating queries with multiple tables, set the `alias` to empty string `""`
or the table name to have it added to the column references. An empty table alias or one equal
to the table name will only be used for column references.

[`Generate Kotlin-Model.groovy`] has been updated to generate the new model format from tables
in the database and optionally use a `model-config.json` to provide table to generated model
file.

### :warning: 0.5.0-beta-6 Adds *profileName* after `db/`

Breaking change in resource `db/` adds profile name after `db/` to allow multi-database
migrations, with `default` being the default profile name.

To migrate previous `db/` structure move all directories other than `templates` under `db/` to
`db/default`

## Overview

A light weight library that exposes JDBC API with the convenience of Kotlin and gets out of the
way when it is not needed.

For developers who prefer to have their database access through SQL where they can read it,
validate it and view its query plans instead of working through cumbersome ORM or limited
frameworks which obfuscate SQL with their non-standard cryptic syntax.

Refactored from [KotliQuery](https://github.com/seratch/kotliquery) which is an excellent idea
but for my use I needed to simplify its implementation to make adding functionality easier
without having to create a ton of intermediate classes, add result set to JSON conversion
without intermediate objects, add stored procedure calls with `in`/`inout`/`out` parameters and
ability to process multiple result sets.

Convenient models with simple syntax which are aware of primary key columns, auto generated
columns, columns with defaults and nullable columns. Models protected property `db` to define
properties via `provideDelegate`. See [Convenient Models](#convenient-models)

```kotlin-a
import java.sql.Timestamp

// dbCase = true if database columns same as properties
// dbCase = false if database columns are snake-case versions of property names
class ValidModel(session:Session? = null, quote:String? = null) : ModelNoData<ValidModel>("tableName", dbCase = true) {
    var processId: Long? by db.autoKey
    var title: String by db
    var version: String by db
    var optional: Int? by db
    var hasOwnColumnName: Int? by db.column("own_name")
    var updatedAt: Timestamp? by db.auto
    var createdAt: Timestamp? by db.auto

    override operator fun invoke() = ValidModel(_session, _quote)
}
```

If you are using [IntelliJ IDEA](https://www.jetbrains.com/idea/) IDE then defining a Language
Injection for the sql factory functions will automatically apply syntax highlighting,
completions and annotations to the SQL strings passed to `sqlQuery()` and `sqlCall()`, making it
even easier to work with SQL queries. See
[Configuring SQL Language Injections](#configuring-sql-language-injections)

IntelliJ Ultimate Database Tools extension script for conversion of SQL tables to a Model is
also available. See
[Installing IntelliJ Ultimate Database Tools Extension Script](#installing-database-tools-extension-scripts).

The library provides a simple migration command processor to implement migrate/rollback
functionality with version tracking with each version containing a copy of database entities:
functions, procedures, tables, triggers and views. See [Migrations](#migrations)

## Getting Started

#### Maven

```maven
<dependency>
    <groupId>com.vladsch.kotlin-jdbc</groupId>
    <artifactId>kotlin-jdbc</artifactId>
    <version>0.5.0-beta-7</version>
</dependency>
```

#### Gradle

```gradle
compile "com.vladsch.kotlin-jdbc:kotlin-jdbc:0.5.0-beta-7"
```

### Example

#### Creating DB Session

`Session` object, thin wrapper of `java.sql.Connection` instance, runs queries, optionally
converts results into instances, lists, hash maps with corresponding json versions as json
objects or json arrays.

```kotlin-a
import com.vladsch.kotlin.jdbc.*

val session = session("jdbc:h2:mem:hello", "user", "pass")
```

#### HikariCP

[HikariCP](https://github.com/brettwooldridge/HikariCP) is an excellent choice for connection
pool implementation. It is blazing fast and easy to use.

```kotlin-a
HikariCP.default("jdbc:h2:mem:hello", "user", "pass")

using(session(HikariCP.dataSource())) { session ->
   // working with the session
}
```

Define default data source for session and use shorter code:

```kotlin-a
HikariCP.default("jdbc:h2:mem:hello", "user", "pass")
// define default data source factory to allow use of session() for default
SessionImpl.defaultDataSource = { HikariCP.dataSource() }

usingDefault { session ->
   // working with the session
}
```

#### DDL Execution

```kotlin-a
session.execute(
sqlQuery("""
  create table members (
    id serial not null primary key,
    name varchar(64),
    created_at timestamp not null
  )
""")) // returns Boolean
```

#### Update Operations

```kotlin-a
val insertQuery: String = "insert into members (name,  created_at) values (?, ?)"

session.update(sqlQuery(insertQuery, "Alice", Date())) // returns effected row count
session.update(sqlQuery(insertQuery, "Bob", Date()))
```

#### Select Queries

Prepare select query execution in the following steps:

- Create `SqlQuery` or `SqlCall` object by using `sqlQuery()` or `sqlCall()` factory
- run the query using `session.list` or `session.first` passing it extractor function (`(Row) ->
  A`)

```kotlin-a
val allIdsQuery = sqlQuery("select id from members")
val ids: List<Int> = session.list(allIdsQuery) { row -> row.int("id") }
```

Extractor function can be used to return any type of result type from a `ResultSet`.

```kotlin-a
data class Member(
  val id: Int,
  val name: String?,
  val createdAt: java.time.ZonedDateTime)

val toMember: (Row) -> Member = { row ->
  Member(
    row.int("id"),
    row.stringOrNull("name"),
    row.zonedDateTime("created_at")
  )
}

val allMembersQuery = sqlQuery("select id, name, created_at from members")
val members: List<Member> = session.list(allMembersQuery, toMember)
```

```kotlin-a
val aliceQuery = sqlQuery("select id, name, created_at from members where name = ?", "Alice")
val alice: Member? = session.first(aliceQuery, toMember)
```

#### Named query parameters

Alternative syntax is supported to allow named parameters in all queries.

```kotlin-a
val query = sqlQuery("""
SELECT id, name, created_at FROM members
WHERE (:name IS NOT NULL OR name = :name) AND (:age IS NOT NULL OR age = :age)
""", mapOf("name" to "Alice"))
```

In the query above, the param `age` is not supplied on purpose.

Performance-wise this syntax is slightly slower to prepare the statement and a tiny bit more
memory-consuming, due to param mapping. Use it if readability is a priority.

This method converts the pattern name to an indexed `?` parameter and is not based on
"artificial" string replacement.

`SqlCall` instances take an additional map of out parameters. The value used for the out
parameter is only significant for its type used when getting the results back. For `inout`
parameters pass the name in both maps.

```kotlin-a
sqlCall("""call storedProc(:inParam,:inOutParam,:outParam)""",
	mapOf("inParam" to "Alice", "inOutParam" to "Bob"),
	mapOf("inOutParam" to "", "outParam" to ""))
```

For convenience there are methods to pass parameters as in, inout, out as a list of pairs or
maps:

```kotlin-a
sqlCall("""call storedProc(:inParam,:inOutParam,:outParam)""")
	.inParams("inParam" to "Alice")
	.inOutParms("inOutParam" to "Bob")
	.outParams("outParam" to "")
```

However, the first method is fastest because it sets all the parameters with the least run-time
processing.

:information_source: parameter-like sequences in single, double or back-quotes are ignored.

##### Collection parameter values

Automatic expansion of `sqlQuery` and `sqlCall` collection valued named parameters to their
contained values will be performed. This allows collections to be used where individual
parameters are specified:

```kotlin-a
sqlQuery("SELECT * FROM Table WHERE column in (:list)").inParams("list" to listOf(1,2,3))
```

Will be expanded to `SELECT * FROM Table WHERE column in (?,?,?)` with parameters of `1, 2, 3`
passed to the prepared statement.

#### Typed params

In the case, the parameter type has to be explicitly stated, there's a wrapper class -
`Parameter` that will help provide explicit type information.

```kotlin-a
val param = Parameter(param, String::class.java)
sqlQuery("""select id, name
    from members
    where ? is null or ? = name""",
    param, param)
```

or also with the helper function `param`

```kotlin-a
sqlQuery("""select id, name
    from members
    where ? is null or ? = name""",
    null.param<String>(), null.param<String>())
```

This can be useful in situations similar to one described
[here](https://www.postgresql.org/message-id/6ekbd7dm4d6su5b9i4hsf92ibv4j76n51f@4ax.com).

#### Working with Datasets

`#forEach` allows you to make some side-effect in iterations. This API is useful for handling a
`ResultSet` one row at a time.

```kotlin-a
session.forEach(sqlQuery("select id from members")) { row ->
  // working with large data set
}
```

As an alternative when you need to modify a small amount of values or columns, and then pass the
results as JSON to the front-end, you can convert the result set to JSON object or array and
modify the data in place.

This library uses the [`boxed-json`](https://github.com/vsch/boxed-json) library's
`MutableJsObject` and `MutableJsArray` which allow modifications to the `JsonValue`s without
having to copy the object.

#### Transactions

`Session` object provides transaction block. Transactions are automatically committed if not
explicitly committed or cancelled inside the block. Any uncaught exceptions will cause the
transaction to be automatically rolled back.

The `Transaction` instance is a session with added `java.sql.Connection` transaction methods for
convenience.

```kotlin-a
session.transaction { tx ->
  // begin
  tx.update(sqlQuery("insert into members (name,  created_at) values (?, ?)", "Alice", Date()))
}

session.transaction { tx ->
  // begin
  tx.update(sqlQuery("update members set name = ? where id = ?", "Chris", 1))
  throw RuntimeException() // rollback
}
```

#### Queries

SQL queries come in two forms: `SqlQuery` for all DDL and DML. `SqlCall` is for calling stored
procedures with in/inout/out parameters to pass to the procedure and processing inout/out
parameters after execution with optional processing of multiple result sets returned by the
procedure.

All queries are executed through the `Session` instance or its sub-class `Transaction`. The
session has separate methods for different types of query execution and results:

* `session.query` used to execute queries and processing a result set
* `session.execute` used to execute queries not expecting a result set
* `session.update` used to execute update queries

Convenience methods that process result sets which use an extractor which take a ResultSet Row
and return an instance of something:

* `session.list` used to return a list of extracted data from rows
* `session.first` used to return a single instance from head of result set
* `session.count` used to return the count of rows when you don't need more than that and are
  too lazy to write a count query
* `session.hashMap` same as list but used to return a hash map keyed on column(s) from the
  result set
* `session.jsonArray` same as list but returns an array of `JsonObjects` holding each row data
* `session.jsonObject` same as `hashMap` except the `JsonObject` first level properties are a
  string of the keyed column(s) from each row.

Iteration helpers which will invoke a consumer for every row of result set data:

* `session.forEach` to iterate over each row or a result set from an `SqlQuery` or `SqlCall`
* `session.forEach` to iterate over each result set from a `SqlCall` and to process inout/out
  parameters.

Update and getting generated key(s):

* `session.updateGetId` to execute an update query and get the first column of the first row of
  the generated keys result set and return its integer value
* `session.updateGetIds` to execute an update query and get the first column of all the rows of
  the generated keys result set and return a list of integers
* `session.updateGetKey` to execute an update query and get the keys (using an extractor) of the
  first row of the generated keys result set and return its value
* `session.updateGetKeys` to execute an update query and get the keys (using an extractor) of
  all the rows of the generated keys result set and return them as a list

#### Convenient Models

A base `Model` class can be used to define models which know how to set their properties from a
`Row` result set row, from other Models, understand `auto` generated, `key` columns and columns
with `default` values; can generate `INSERT`, `UPDATE`, `DELETE` and `SELECT` queries for a
model instance with validation of required fields and minimal required columns for an update.

Using these models is a convenience not a requirement since it does create overhead by building
the model's properties for every instance.

* Define model's properties by using `by db`. The nullability of the property type dictates
  whether the property can be omitted or set to null
* Key properties by: `by db.key`. These will be used for `WHERE` list for `UPDATE`, `DELETE` or
  `SELECT` for reload query generation
* Auto generated (left out of update and insert column list) properties by: `by db.auto`
* Auto generated Key (key column and auto generated) by: `by db.key.auto`, `by db.autoKey` or
  `by db.auto.key`
* Columns which have default values by: `by db.default`. These won't raise an exception for
  `INSERT` query generation if they are missing from the model's defined property set. A
  function alternative `by db.default(value)` will provide a default value which will be used
  for insert query if an explicit value is not provided for the property.

If column names are the same as the property names then set `dbCase = true` for the `Model`
constructor argument. If column names are snake-case versions of camelcase property names
(lowercase with underscores between words) then set `dbCase = false` and the model will generate
correct column names automatically. When needed, you can provide a column name explicitly via
`.column("columnName")` making it independent of the property name. This function is available
on any delegate provider so it can be combined with key, auto and default properties: `model`,
`model.key`, `model.auto`, `model.default`

By default models allow public setters on properties marked `auto` or `autoKey`. To add
validation forcing all `auto` properties to have no `set` method or have `private set` pass
`false` for `allowSetAuto` second parameter to model constructor.

Any property marked as `auto` generated will not be used in `UPDATE` or `INSERT` queries.

##### Identifier Quoting

Each model is attached to a session. Column and table names are quoted by default using the
connection's `metaData.identifierQuoteString`.

##### Model Generation

For IntelliJ Ultimate a Database extension script can be installed which will generate models
from the context menu of any table in the database tools window. See
[Installing Database Tools Extension Scripts](#installing-database-tools-extension-scripts)

Result set row to model/json/data conversion:

* `val toModel: (Row) -> M = toModel()`
* `val toData: (Row) -> D = toData()`
* `val toJson: (Row) -> JsonObject = toJson()`

These return conversion function with identifier quoting option:

**These need updating for version 0.5**

List Query Helpers:

* `fun listQuery(vararg params: Pair<String, Any?>, quote:String =
  ModelProperties.databaseQuoting): SqlQuery`
* `fun listQuery(params: Map<String, Any?>, quote:String = ModelProperties.databaseQuoting):
  SqlQuery`
* `fun listQuery(whereClause:String, vararg params: Pair<String, Any?>, quote:String =
  ModelProperties.databaseQuoting): SqlQuery`
* `fun listQuery(whereClause:String, params: Map<String, Any?>, quote:String =
  ModelProperties.databaseQuoting): SqlQuery`

List results:

* `fun list(session: Session, vararg params: Pair<String, Any?>, quote:String =
  ModelProperties.databaseQuoting): List<D>`
* `fun list(session: Session, params: Map<String, Any?>, quote:String =
  ModelProperties.databaseQuoting): List<D>`
* `fun list(session: Session, whereClause:String, vararg params: Pair<String, Any?>,
  quote:String = ModelProperties.databaseQuoting): List<D>`
* `fun list(session: Session, whereClause:String, params: Map<String, Any?>, quote:String =
  ModelProperties.databaseQuoting): List<D>`

JSON Array results:

* `fun jsonArray(session: Session, vararg params: Pair<String, Any?>, quote:String =
  ModelProperties.databaseQuoting): JsonArray`
* `fun jsonArray(session: Session, params: Map<String, Any?>, quote:String =
  ModelProperties.databaseQuoting): JsonArray`
* `fun jsonArray(session: Session, whereClause:String, vararg params: Pair<String, Any?>,
  quote:String = ModelProperties.databaseQuoting): JsonArray`
* `fun jsonArray(session: Session, whereClause:String, params: Map<String, Any?>, quote:String =
  ModelProperties.databaseQuoting): JsonArray`

```kotlin
    data class ValidData(
        val processId: Long?,
        val noSetter: String,
        val noSetter2: String,
        val title: String,
        val version: String,
        val unknown: String?,
        val createdAt: String?,
        val createdAt2: String?
    )

    class ValidModel(session: Session? = session(), quote: String? = null) : Model<ValidModel, ValidData>(session, tableName, true, false, quote) {

        var processId: Long? by db.key.auto; private set
        val noSetter: String by db.auto
        val noSetter2: String by db.autoKey
        var title: String by db
        var version: String by db
        var unknown: String? by db
        var createdAt: String? by db.auto; private set
        val createdAt2: String? by db.auto

        override fun invoke(): ValidModel {
            return ValidModel(_session, _quote)
        }

        override fun toData(): ValidData {
            return ValidData(processId, noSetter, noSetter2, title, version, unknown, createdAt, createdAt2)
        }

        companion object {
            const val tableName = "TableName"
        }
    }


fun useModel() {
    using(session(HikariCP.default())) { session ->
        // get all rows from table as list of the data class
        val modelList = ValidModel(session).listData()

        val model = ValidModel(session)
        model.title = "title text"
        model.version = "V1.0"

        // execute an insert and set model's key properties from the keys returned by the database
        // batch will be set to 1 since it is not set in properties
        model.insert()

        // this will delete the model and clear auto.key properties
        model.delete()

        // this will delete the model but not clear auto.key properties
        model.deleteKeepAutoKeys()

        // execute select query for model (based on keys) and load model
        model.select()

        // just insert, don't bother getting keys
        model.insertIgnoreKeys()

        // take a snapshot of current properties
        model.snapshot()
        model.version = "V2.0"

        // will only update version since it is the only one changed, does automatic snapshot after update
        model.update()

        // will only update version since it is the only one changed but will reload model from database
        // if updatedAt field is timestamped on update then it will be loaded with a new value
        model.version = "V3.0"
        model.updateReload()
    }
}
```

### IntelliJ Configuration

#### Configuring SQL Language Injections

You can manually add `@Language("SQL")` annotation to strings or add a language injection
configuration:

The places patterns text is:

```
+ kotlinParameter().ofFunction(0, kotlinFunction().withName("appendWhereClause"))
+ kotlinParameter().ofFunction(0, kotlinFunction().withName("sqlCall").definedInPackage("com.vladsch.kotlin.jdbc"))
+ kotlinParameter().ofFunction(0, kotlinFunction().withName("sqlQuery").definedInPackage("com.vladsch.kotlin.jdbc"))

```

![Language-Injections](assets/images/Language-Injections.png)

To get full benefit of SQL completions you should also define a database source to the database
against which you are developing (or a local clone of it) and configure the SQL dialect for the
database you are using.

#### Installing Database Tools Extension Scripts

:information_source: Database Tools are available on IntelliJ Ultimate and other IDEs but not in
the IntelliJ Community Edition

##### Generate Kotlin Models from Tables Script

Download the groovy script for generating a `kotlin-jdbc` model:
[Generate Kotlin-Model.groovy](extensions/com.intellij.database/schema/Generate%20Kotlin-Model.groovy)

In database tool window, right click on a table and select Scripted Extensions > Go to scripts
directory and copy the script to this location.

![Scripted Extensions Goto Script Dir](assets/images/Scripted_Extensions_Goto_Script_Dir.png)

It will appear in the `Scripted Extensions` pop-up menu. For best results use the native schema
introspection instead of JDBC in connection configuration.

![Scripted Extensions Generate Kotlin Model](assets/images/Scripted_Extensions_Generate_Kotlin-Model.png)

If the a file `model-config.json` file exists in output directory or its along the parent path
then it will be used for determining the models actual output file and package.

For example:

```json
{
  "package-prefix" : "",
  "remove-prefix" : "gen/main/kotlin/",
  "skip-unmapped" : false,
  "file-map": {
    "play_evolutionModel.kt": "",
    "migrationModel.kt": "",

    "ProcessInstanceModel.kt": "gen/main/kotlin/com/vladsch/kotlin/models/process/ProcessInstanceModel.kt",
    "ProcessModel.kt": "gen/main/kotlin/com/vladsch/kotlin/models/process/ProcessModel.kt",

    "": "gen/main/kotlin/com/vladsch/kotlin/models/"
  }
}
```

* `remove-prefix` if present and matches the mapped file name prefix, will be removed from the
  mapped file before prefixing with `package-prefix`
* `package-prefix` if present will be prefixed to the generated package name
* `skip-unmapped`, if `true`, any model names not present in the mapped will not be generated,
  if `false` the models will be generated to the output directory. Ignored if empty file name
  mapping exists.
* `file-map`, map of model name to file path relative to `model-config.json` file location.
  * an empty name entry will match any file not explicitly matched by other entries and allows
    directing unmapped entries to a default location.
  * if a model is mapped to an empty name then this model will not be generated.
* script hardcoded parameters can also be changed in the config file to eliminate the need to
  edit the script. If no value provided then script default will be used:
  * `classFileNameSuffix`, default `"Model"`, appended to class file name
  * `downsizeLongIdToInt`, default `true`, if `true` changes id columns which would be declared
    `Long` to `Int`, change this to false to leave them as `Long`
  * `fileExtension`, default `".kt"`, model extension
  * `forceBooleanTinyInt`, default "", regex for column names marked as boolean when tinyint,
    only needed if using jdbc introspection which does not report actual declared type so all
    `tinyint` are `tinyint(3)`
  * `snakeCaseTables`, default false, if true convert snake_case table names to Pascal case,
    else leave as is
  * `indent`, default 4 spaces, string to use for each indent level

:information_source: the easiest way to generate the file-map is first generate models with
default mapping the file map. Move them to the desired sub-directories and then use the IDE
`Copy Relative Path` context menu action and multi-caret editing or or a script to generate the
mapping from existing directory structure and content. If any tables are added in the future
they will automatically generate in the root and can be moved to the desired sub-directory and
mapping added to the file-map.

Will not generate models for tables `play_evolutions` and `migrations`, will output `AuditLogs`
table model to `app/audit/models/` subdirectory with package set to `app.audit.models`

All other tables, if selected will be generated to the output directory with package set to
`com.sample`

The intended use case is to have a generated models directory with the configuration file and
all generated models in the project directory. When generating models, select any sub-directory
of the project and the files will be generated in the correct location, especially if default
file location mapping was provided.

If you need to modify the models after they are generated, it is best to copy the auto-generated
models to another directory as the source used in the project. Subsequent auto-generated models
should still be generated into the same directory and compared and/or merged into manually
changed model using the compare directory/file action of the IDE.

##### Generate Scala Slick Models from Tables

Add [`Generate Scala-Slick-Model.groovy`] for generating a Scala/Slick database model.

* In addition to the Kotlin model generator `model-config.json` configuration values Scala model
  generator has additional configuration properties to control model generation:
  * `classFileNameSuffix`, default `"Model"`, appended to class file name
  * downsizeLongIdToInt, default `true`, if true changes id columns which would be declared
    `Long` to `Int`, change this to `false` to leave them as `Long`
  * `fileExtension`, default `".scala"`, model extension
  * `forceBooleanTinyInt`, default "", regex for column names marked as boolean when tinyint,
    only needed if using jdbc introspection which does not report actual declared type so all
    `tinyint` are `tinyint(3)`
  * `snakeCaseTables`, default `false`, if `true` convert snake_case table names to Pascal case,
    else leave as is
  * `indent`, default 2 spaces, string to use for each indent level
  * `addToApi`, default `true`, create database Model class with `Date`/`Time`/`Timestamp` field
    types and an Api class with `String` data types for these fields. Intended to be used for
    converting to/from JSON when communicating with a JavaScript client. The Api class has
    methods `toModel()` and `fromModel()` to easily convert between Api and database model
    class.

    :information_source:The Api class will only be created if there are date/time fields in the
    model.

  * `apiFileNameSuffix`, default `"Gen"`, appended to file name for the generated Api class.
  * `convertTimeBasedToString`, default `false`, to convert all date, time and timestamp to
    String in the model

##### Result Set Conversion Scripts

[`Kotlin-Enum.kt.js`] to convert result set data to Kotlin Enum definition. You need to add it
to the `data/extractors` directory

![Scripted_Extensions_Generate-Kotlin-Model](assets/images/Scripted_Extensions_Data_Extractors.png)

It uses the first column name as the enum name (id suffix stripped and last word pluralized).
The first column which contains all non-numeric values will be used for names of the enum values
(converted to screaming snake case), if no such column exists then the names will be the
screaming snake case of the enum name with id values appended.

All columns will be included in the enum value constructor and search functions in companion
object for an enum value of a given column. Easier to show than explain:

Result set:

| changeHistoryTypeId |      type       |
|---------------------|-----------------|
| 1                   | Process         |
| 2                   | File            |
| 3                   | Client          |
| 4                   | User            |
| 5                   | ProcessInstance |

Generated Kotlin enum:

```kotlin
enum class ChangeHistoryTypes(val id: Int, val type: String) {
  PROCESS(1, "Process"),
  FILE(2, "File"),
  CLIENT(3, "Client"),
  USER(4, "User"),
  PROCESS_INSTANCE(5, "ProcessInstance");

  companion object {
    fun changeHistoryTypeId(id: Int): ChangeHistoryTypes? = values().find { it.id == id }
    fun type(type: String): ChangeHistoryTypes? = values().find { it.type == type }
  }
}
```

A script for generating a JavaScript enum based on [`enumerated-type`] npm package will generate
an enum usable in JavaScript [`JavaScript-Enumerated-Value-Type.js`]

A script for generating a markdown table for the table data [`Markdown-Table.md.js`]. The table
above was generated with this script.

## Migrations

:warning: Migrations require specific database details, which in this library is provided by
`DbEntityExtractor` interface. Currently only MySql version is implemented by
`MySqlEntityExtractor` limiting migration functionality to MySql data sources.

Migrations are implemented by the `Migrations.dbCommand(String[])` function. `Migrations`
constructor is provided with the database session, `DbEntityExtractor` instance and
resourceClass instance holding the database migration version resource files.

When a migration command is run for the first time, it will create a `migration` table where all
migration operation will be stored and used for determining which operations should be performed
to bring the database to a specific version.

Each version of the database entities is stored in a sub-directory with the version format:
Vv_m_p_meta, where v is version number integer, m is minor version integer, p is patch version
integer and meta is any string. Only the Vv portion is required. Minor, patch and meta are
optional. The underscore separating version parts belongs to the next element. i.e. the correct
version is `V1` not `V1_`, `V1_2` and not `V1_2_`, etc.

Each profile/version has the following directory structure and database entity script naming
conventions:

```
db/
└── profileName
    └── schema
    └── V0_0_0
        ├── functions
        │   └── sample-function.udf.sql
        ├── migrations
        │   ├── 0.sample-migration.down.sql
        │   └── 0.sample-migration.up.sql
        ├── procedures
        │   └── sample-stored-procedure.prc.sql
        ├── tables
        │   └── sample-table.tbl.sql
        ├── triggers
        │   └── sample-trigger.trg.sql
        └── views
            └── sample-view.view.sql
```

Database Entities:

| Database Entity  | Directory  | File Extension |
|------------------|------------|----------------|
| function         | functions  | `.udf.sql`     |
| stored procedure | procedures | `.prc.sql`     |
| table            | tables     | `.tbl.sql`     |
| trigger          | triggers   | `.trg.sql`     |
| view             | views      | `.view.sql`    |

All entity scripts for a particular version will be run when the database is migrated (up or
down) to that version as the final version. Any entities in the database which do not have a
corresponding script file, will be deleted from the database.

For example, if the database is migrated from `V1` to `V5` with intermediate versions: `V2`,
`V3` and `V4` then up migration scripts for versions `V2`, `V3`, `V4` and `V5` will be run and
only the scripts for database entities of `V5` will be run.

Migration Scripts:

Both up/down migration scripts are located in the `migrations` directory and are distinguished
by their extension: `.up.sql` and `.down.sql`.

The files in this directory are executed in sorted order and all files should have an integer
prefix, optionally followed by descriptive text. Files which have an integer prefix will be
sorted in numerical order, otherwise alphabetic order.

When applying up migration scripts these are executed in increasing file name order.

During a rollback operation, only down scripts whose up script execution has been recorded in
the `migrations` table will be executed. All down scripts are executed in decreasing file name
order.

Migration script files are split on `;` delimiters and each part run as a separate query, if
successful then an entry for this fact is added to the migration table.

After all migrations/rollback scripts have been applied for all required versions, the database
entity scripts (excluding tables) for the resulting version will be run. This means that
migrations are only required to migrate the table schema and table data. Other entities will be
updated via their own scripts.

:warning: The migration scripts should not assume a particular version of other entities than
tables because function, procedure, view or trigger scripts will only be applied after all
migration/rollback scripts for all intervening versions are run and only the entity scripts for
the final version will be executed. If you require as specific version of these entities in the
migration scripts then you will need to include these in the migration scripts.

After migration/rollback it is a good idea to run `update-schema` command to copy all the entity
scripts to the `schema` directory, located on the same level as the version sub-directories.
Contents of this directory are intended to be under VCS and used to track changes to database
from version to version. Individual version directories are immutable for a given version and
therefore not useful for VCS based modification tracking tools.

To make debugging of rollback/migration scripts easier, after each migration/rollback the
resulting database tables are validated against the corresponding version's `tables/` directory
contents and an error is recorded if the validation fails. The validation will ignore
differences caused by re-ordering of lines, this is used to eliminate column, key and constraint
order changes from causing validation failures.

To generate contents for the `tables/` directory of the current version or a specific version,
run the `dump-tables` command.

Commands:

* path "resources/db" - set path to resources/db directory of the project where version
  information is stored.

* version "versionID" - set specific version for following commands

  "versionID" must be of the regex form `V\d+(_\d+(_\d+(_.*)?)?)?`

  where the `\d+` are major, minor, patch versions with the trailing `.*` being the version
  metadata. Versions are compared using numeric comparison for major, minor and patch.

  The metadata if present will be compared using regular string comparison, ie. normal sort.

* profile "profileName" - set specific db profile name to use for the commands

* init - initialize migrations table and migrate all to given version or latest version based on
  database table match to table schemas contained in versions

  if profile is not given then will init all defined profiles

* new-major - create a new version directory with major version incremented, from current or
  requested version.

  specific profile name is required

* new-minor - create a new version directory with minor version incremented, from current or
  requested version.

* new-patch - create a new version directory with patch version incremented, from current or
  requested version.

  specific profile name is required

* new-version - create a new version directory for the requested version. The directory cannot
  already exist. If the version is not provided then the current version with its patch version
  number incremented will be used.

  All entity directories will be created, including migrations.

  If there is a previous version to the one requested then all its entity scripts will be copied
  to the new version directory.

  specific profile name is required

* import-evolutions "play/evolutions/directory" "min" "max". Import evolutions converting them
  to migrations in the current version. `min` is the minimum evolution to import. `max` is
  optional and if provided gives the maximum evolution number to import.

  specific profile name is required

* new-evolution "play/evolutions/directory" create a new play evolution file from current or
  requested version migrations and rollbacks in the requested directory.

  specific profile name is required

* new-migration "title" - create a new up/down migration script files in the requested (or
  current) version's migrations directory. The file name will be in the form: N.title.D.sql
  where N is numeric integer 1..., D is up or down and title is the title passed command.
  Placeholders in the file: `__VERSION__` will be replaced with the version for which this file
  is generated and `__TITLE__` with the "title" passed to the command.

  profile name defaults to `default` if one is not given

* new-function "name" - create a new function file using resources/db/templates customized
  template or built-in if none Placeholders in the file: `__VERSION__` will be replaced with the
  version for which this file is generated and `__NAME__` with the "name" passed to the command.

  profile name defaults to `default` if one is not given

* new-procedure "name" - create a new procedure file using resources/db/templates customized
  template or built-in if none Placeholders in the file: `__VERSION__` will be replaced with the
  version for which this file is generated and `__NAME__` with the "name" passed to the command.

  profile name defaults to `default` if one is not given

* new-trigger "name" - create a new trigger file using resources/db/templates customized
  template or built-in if none Placeholders in the file: `__VERSION__` will be replaced with the
  version for which this file is generated and `__NAME__` with the "name" passed to the command.

  profile name defaults to `default` if one is not given

* new-view "name" - create a new view file using resources/db/templates customized template or
  built-in if none Placeholders in the file: `__VERSION__` will be replaced with the version for
  which this file is generated and `__NAME__` with the "name" passed to the command.

* migrate - migrate to given version or to latest version

  applies command to all defined profiles if profile name is not given

* rollback - rollback to given version or to previous version

  specific profile name is required

* dump-tables - dump database tables

  applies command to all defined profiles if profile name is not given

* create-tables - create all tables which exist in the version tables directory and which do not
  exist in the database

  applies command to all defined profiles if profile name is not given

* validate-tables - validate that version table scripts and database agree

  applies command to all defined profiles if profile name is not given

* update-all - update all: functions, views, procedures, triggers. This runs the scripts
  corresponding to the database object for the requested version.

  applies command to all defined profiles if profile name is not given

* update-procedures update-procs - update stored procedures

  applies command to all defined profiles if profile name is not given

* update-functions update-funcs - update functions

  applies command to all defined profiles if profile name is not given

* update-triggers - update triggers

  applies command to all defined profiles if profile name is not given

* update-schema - update `schema` directory with entities from selected version (or current if
  none given)

  applies command to all defined profiles if profile name is not given

* update-views - update views

  applies command to all defined profiles if profile name is not given

* exit - exit application

#### Customizing Templates used by `new-...` command

Place your files in the `resources/db` directory and name it `templates` the layout is the same
as a version directory with the template files named `sample`:

```text
db/
└── templates/
    ├── functions/
    │   └── sample.udf.sql
    ├── migrations/
    │   ├── 0.sample.down.sql
    │   └── 0.sample.up.sql
    ├── procedures/
    │   └── sample.prc.sql
    ├── triggers/
    │   └── sample.trg.sql
    └── views/
        └── sample.view.sql
```

## License

(The MIT License)

Copyright (c) 2015 - Kazuhiro Sera

Copyright (c) 2018-2019 - Vladimir Schneider

[`enumerated-type`]: https://github.com/vsch/enumerated-type
[`Generate Kotlin-Model.groovy`]: https://github.com/vsch/kotlin-jdbc/blob/master/extensions/com.intellij.database/schema/Generate%20Kotlin-Model.groovy
[`Generate Scala-Slick-Model.groovy`]: extensions/com.intellij.database/schema/Generate%20Scala-Slick-Model.groovy
[`JavaScript-Enumerated-Value-Type.js`]: extensions/com.intellij.database/data/extractors/JavaScript-Enumerated-Value-Type.js
[`Kotlin-Enum.kt.js`]: extensions/com.intellij.database/data/extractors/Kotlin-Enum.js
[`Markdown-Table.md.js`]: extensions/com.intellij.database/data/extractors/Markdown-Table.md.js
[enumerated-type]: https://github.com/vsch/enumerated-type

