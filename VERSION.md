# Version History

[TOC]: # " "

- [TODO](#todo)
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

