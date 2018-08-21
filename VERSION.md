# Version History

[TOC]: # " "

- [TODO](#todo)
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

