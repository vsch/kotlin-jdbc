# Version History

[TOC]: # " "

- [TODO](#todo)
    - [High Priority](#high-priority)
- [0.5.0 API Breaking Release](#050-api-breaking-release)
- [0.4.10](#0410)
- [0.4.8](#048)
- [0.4.6](#046)
- [0.4.4](#044)
- [0.4.2](#042)
- [0.4.0](#040)
- [0.3.4](#034)
- [0.3.2](#032)
- [0.3.0](#030)
- [0.2.26](#0226)
- [0.2.24](#0224)
- [0.2.22](#0222)
- [0.2.20](#0220)
- [0.2.18](#0218)
- [0.2.16](#0216)
- [0.2.14](#0214)
- [0.2.12](#0212)
- [0.2.10](#0210)
- [0.2.8](#028)
- [0.2.6](#026)
- [0.2.4](#024)
- [0.2.2](#022)
- [0.2.0](#020)
- [0.1.8](#018)
- [0.1.6](#016)
- [0.1.4](#014)
- [0.1.2](#012)
- [0.1.0](#010)


## TODO

* [ ] Add: tests for all type conversions
  * [ ] Result set
  * [ ] Json
  * [ ] Kotlin types
* [ ] Add case sensitive database entity aware migration
  * [ ] Tables
  * [ ] Others?

### High Priority

* [ ] Test: query generating functions
  * [ ] validate quote parameter is used for table and column names
  * [ ] alias parameter
    * [ ] `""`
    * [ ] same as table name
    * [ ] other string `"a"`
* [ ] Test: list generation functions
* [ ] Fix: Readme docs, consider creating a wiki

## 0.5.0 API Breaking Release

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
in the database.

## 0.4.10

* Fix: merge #7, isLast and isAfterLast checking removed for TYPE_FORWARD_ONLY ResultSet thanks
  to [@s5r3](https://github.com/s5r3)

## 0.4.8

* Fix: migrate was using string compare for current version and versions for finding later ones
* Fix: parameter extraction should take quoted, double quoted and back-quoted strings into
  account. Otherwise a string with `:\w+` will be treated as a parameter.

## 0.4.6

* Fix: migrate command was string sorting versions instead of using versionCompare()

## 0.4.4

* Add: multiple list parameter handling, PR by **[root-talis](https://github.com/root-talis)**

## 0.4.2

* Fix: factor out `Session` and `Transaction` functions into interfaces. Implementation renamed
  to `SessionImpl` and `TransactionImpl`

## 0.4.0

* Fix: remove database dependent quoting of table name and column names and replace it with an
  optional `quote` argument to `ModelProperties` and `Model` constructor argument with default
  from `ModelProperties.databaseQuoting` which can be set to any desired string. If the same
  database type is used on all connections then quoting should be set during initialization
  through `Model.databaseQuoting`.

  However, if multiple databases with different quoting requirements are used then `quote`
  argument will need to be passed to model constructor based on the database connection.

  * companion functions requiring quoting now take an optional argument `quote` with default of
    `ModelProperties.databaseQuoting`
  * model companion abstract `createModel()` now has `createModel(quote: String?)` signature so
    it can pass `quote` argument to model constructor.

* Fix: list query with where clause parameters to use name parameters instead of plain
  parameters since parameters are passed as a map.

* Fix: list queries with where clause parameters having collection values to use `IN (:name)`
  instead of `name = :name` for condition

* Add: automatic expansion of `sqlQuery` and `sqlCall` collection valued parameters into their
  contained values. This allows lists to be used where individual parameters are expected:

```kotlin
sqlQuery("SELECT * FROM Table WHERE column in (:list)").inParams("list" to listOf(1,2,3))
```

To expand to `SELECT * FROM Table WHERE column in (?,?,?)` with parameters of `1, 2, 3`.

* Change: refactor `sqlQuery` to substitute named parameters and compute named parameter mapping
  as needed and after any parameter changes. Required to have access to the parameter's type to
  figure out if it is a list or a single value.

## 0.3.4

* Add: variations of `list` and `listJson` to `ModelCompanion` taking where clause and vararg
  `Pair` or map argument for parameters for the where clause for more flexibility of generating
  list queries.

## 0.3.2

* Add: `Model.appendWhereClause()` to simplify creating list queries

## 0.3.0

* Change: from `by model` to `by db` for model property definitions
* Add: `Model.listQuery` companion methods to list model instances with or without conditions
* Add: `ModelCompanion` to make companion methods of real models simpler to update without code
  editing
* Add: separate session for updating migrations table to keep separate from schema modification
  session.
* Fix: add trimming of table sql string for comparing by line re-ordering before splitting into
  lines.
* Fix: JS enum type script for better WebStorm code completion functionality

## 0.2.26

* Add: copying any extra files under `/db/template` resources directory to new version directory
  on creation. Intended for things like change notes or other common file templates.
* Fix: `model`: `auto`, `key`, `default`, `autoKey` combinations
* Add: `model.default(Any?)` default property type definition with a provided default value.
  Will set properties not provided or if their value is null on insert to this value
* Add: insert and update queries to skip passing `null` value parameters which have defaults.
  `null` effectively means the same thing as not setting the parameter, using the db defined
  default instead for insert and no-op for update.
* Add: update query which has no non-null non-default modified parameters will update key column
  to itself to trigger auto updated fields.
* Add: migration verbose `-v` and detailed `-d` flags for table validation
* Add: ignoring named param pattern in query lines that are commented out with `-- ` or `#` as
  first non-blank of the line. Otherwise too many parameters will be passed for the query since
  SQL will not see the named param uses or comments in sql might me interpreted as parameter
  names.
* Add: IntelliJ Ultimate Database Tools data extraction script to generate JavaScript enum from
  table data. See:
* Add: IntelliJ Ultimate Database Tools data extraction script to generate Kotlin enum from
  table data. See:
  [Installing IntelliJ Ultimate Database Tools Extension Scripts](README.md#installing-intellij-ultimate-database-tools-extension-scripts)
* Fix: table validation for out of order lines to ignore trailing , otherwise the last line out
  of order does not match.
* Fix: init command when run on a database without `migrations` table will use db versions table
  schema to determine latest matched version as the current version
* Add: validation for table names in table sql to match file name when doing init.
* Fix: table validation to properly detect when resource is missing
* Add: migrations to play evolutions conversion command: `new-evolution
  "play/evolutions/directory"` which will convert the current or requested version migrations
  and rollbacks to the next play evolutions file in the requested directory.

## 0.2.24

* Fix: model generating script to convert table name to singular form for use as the model class
  name.
* Add: `new-...` commands for: function, procedure, trigger and view.
* Add: built in templates for creating new entities with placeholders `__VERSION__` replaced by
  the version for which they are created and `__TITLE__` for migration/rollback or `__NAME__`
  for all others to reflect the parameter passed to the command

## 0.2.22

* Change: `usingSession` to `usingDefault` as a short form of `using(session())`

## 0.2.20

* Add: `Session.defaultDataSource` lambda to generate default data source for use with
  `session()` version of the function to reduce default session arguments

## 0.2.18

* Fix: commit before doing `System.exit(1)` for exit command

## 0.2.16

* Fix: migrations rollback filter condition reversed for multiple version rollback

## 0.2.14

* Add: migrations `update-schema` command to copy all current version entities to the `schema`
  version directory to allow VCS version to version change tracking.

## 0.2.12

* Fix: npe when trying to access non existent resource

## 0.2.10

* Add: migration convenience commands
  * new-major - create a new version directory with major version incremented.

  * new-minor - create a new version directory with minor version incremented.

  * new-patch - create a new version directory with patch version incremented.

  * new-version - create a new version directory with minor version incremented. The directory
    cannot already exist. If the version is not provided then the next version with its minor
    version number incremented will be used.

    All entity directories will be created, including migrations.

    If there is a previous version to the one requested then its entity scripts will be copied
    to the new version directory, including tables which are used for validation after
    rollback/migration

  * new-migration "title" - create a new up/down migration script files in the requested (or
    current) version's migrations directory. The file name will be in the form: N.title.D.sql
    where N is numeric integer 1..., D is up or down and title is the title passed command.

## 0.2.8

* Add: `Model(val sqlTable: String, dbCase: Boolean, allowSetAuto: Boolean = true)`, `dbCase` to
  mark all properties without specific column name as having database case. When false, database
  columns are assumed to be (snake case) versions of the property names. Available on all
  property providers: `auto`, `key`, `default`

* Add: `model.column(String)` to provide arbitrary column name for the property. Available on
  all property providers: `auto`, `key`, `default`

## 0.2.6

* Add: migration table compare will match if differences are out of order lines between two
  tables. This addresses validation failure for tables which are changed due to rollback causing
  keys and constraints to be in a different order.

## 0.2.4

* Add: `Model.toJson()` method for converting a model to `JsonObject`

## 0.2.2

* Fix: migration command to not apply versions higher than requested
* Add: IntelliJ Database script extension to create Models from database tables
* Fix: Apply all migrations that exist from start. First version under migrations should have
  initial database creation without migrations.
* Change: refactor code to clean up duplication
* Add: `Model.appendKeys()` to append model's keys for query generation
* Fix: wrong conversion for jodaLocalDateTime and a few others. Added casts to catch these
  errors during compilation
* Fix: add snapshot to `Model.update(session:Session)` after successful update.

## 0.2.0

* Add: abstract `Model` class for creating models to reduce boiler plate code.

## 0.1.8

* Fix: wrong filter on get entity resource files list
* Change: change `updateGetId` and `updateGetIds` to return `Int` and `List<Int>` respectively
* Add: add `updateGetLongId` and `updateGetLongIds` to return `Long` and `List<Long>`
  respectively

## 0.1.6

* Fix: iteration over empty result set caused exception, due to insufficient conditions present
  in result set to know there is no next row until next() is invoked.
* Add: rudimentary SQL based migrations with migrate/rollback with up/down sql scripts, table,
  procedure, function, view and trigger script running. Migrations are for data and tables. The
  rest of the scripts are applied for a given version. Needs docs and tests.

## 0.1.4

* Fix: pom packaging to jar

## 0.1.2

* Fix: pom missing info
* Fix: pom and travis errors
* Fix: pom for dokka plugin javadoc generation

## 0.1.0

* Initial release

[`Generate Kotlin-Model.groovy`]: https://github.com/vsch/kotlin-jdbc/blob/master/extensions/com.intellij.database/schema/Generate%20Kotlin-Model.groovy

