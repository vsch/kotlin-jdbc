## Kotlin-JDBC

[![Build Status](https://travis-ci.org/vsch/kotlin-jdbc.svg)](https://travis-ci.org/vsch/kotlin-jdbc)

A thin library that exposes JDBC API with the convenience of Kotlin and gets out of the way when
not needed. When working with a relational database my preference is not to obfuscate SQL with cryptic
and cumbersome ORM or syntactic sugar wrappers. I prefer my SQL to be there front and center
where I can read it, validate it, view query plans to optimize the queries and indexes.

Refactored from [KotliQuery](https://github.com/seratch/kotliquery) which is an excellent idea
but for my use I needed to simplify its implementation to make adding functionality easier
without having to create a ton of intermediate classes, add result set to JSON conversion
without intermediate objects, add stored procedure calls with `in`/`inout`/`out` parameters and
ability to process multiple result sets.

If you are using [IntelliJ IDEA](https://www.jetbrains.com/idea/) IDE then defining a Language
Injection for the sql factory functions will automatically apply syntax highlighting,
completions and annotations to the SQL strings, making it even easier to work with SQL queries.
See [Configuring SQL Language Injections](#configuring-sql-language-injections)

### Getting Started

https://github.com/vsch/kotlin-jdbc/master/sample

#### Maven

```maven
<dependency>
    <groupId>com.vladsch.kotlin-jdbc</groupId>
    <artifactId>kotlin-jdbc</artifactId>
    <version>0.1.2</version>
</dependency>
```

#### Gradle

```maven
<dependency>
    <groupId>com.vladsch.kotlin-jdbc</groupId>
    <artifactId>kotlin-jdbc</artifactId>
    <version>0.1.2</version>
</dependency>
```

### Example

#### Creating DB Session

`Session` object, thin wrapper of `java.sql.Connection` instance, runs queries.

```kotlin
import com.vladsch.kotlin.jdbc.*

val session = sessionOf("jdbc:h2:mem:hello", "user", "pass") 
```

#### HikariCP

[HikariCP](https://github.com/brettwooldridge/HikariCP) is an excellent choice for connection
pool implementation. It is blazing fast and easy to use.

```kotlin
HikariCP.default("jdbc:h2:mem:hello", "user", "pass")

using(session(HikariCP.dataSource())) { session ->
   // working with the session
}
```

#### DDL Execution

```kotlin
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

```kotlin
val insertQuery: String = "insert into members (name,  created_at) values (?, ?)"

session.update(sqlQuery(insertQuery, "Alice", Date())) // returns effected row count
session.update(sqlQuery(insertQuery, "Bob", Date()))
```

#### Select Queries

Prepare select query execution in the following steps:

- Create `SqlQuery` or `SqlCall` object by using `sqlQuery()` or `sqlCall()` factory
- run the query using `session.list` or `session.first` passing it extractor function (`(Row) ->
  A`)

```kotlin
val allIdsQuery = sqlQuery("select id from members")
val ids: List<Int> = session.list(allIdsQuery) { row -> row.int("id") }
```

Extractor function can be used to return any type of result type from a `ResultSet`.

```kotlin
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

```kotlin
val aliceQuery = sqlQuery("select id, name, created_at from members where name = ?", "Alice")
val alice: Member? = session.first(aliceQuery, toMember)
```

#### Named query parameters

Alternative syntax is supported to allow named parameters in all queries.

```kotlin
sqlQuery("""select id, name, created_at 
	from members 
	where (:name is not null or name = :name)
	  and (:age is not null or age = :age)""", 
	mapOf("name" to "Alice"))
```

In the query above, the param `age` is not supplied on purpose.

Performance-wise this syntax is slightly slower to prepare the statement and a tiny bit more
memory-consuming, due to param mapping. Use it if readability is a priority.

This method converts the pattern name to an indexed `?` parameter and is not based on
"artificial" string replacement.

`SqlCall` instances take an additional map of out parameters. The value used for the out
parameter is only significant for its type used when getting the results back. For `inout`
parameters pass the name in both maps.

```kotlin
sqlCall("""call storedProc(:inParam,:inOutParam,:outParam)""",
	mapOf("inParam" to "Alice", "inOutParam" to "Bob"), 
	mapOf("inOutParam" to "","outParam" to ""))
```

For convenience there are methods to pass parameters as in, inout, out as a list of pairs or
maps:

```kotlin
sqlCall("""call storedProc(:inParam,:inOutParam,:outParam)""")
	.inParams("inParam" to "Alice")
	.inOutParms("inOutParam" to "Bob") 
	.outParams("outParam" to "")
```

However, the first method is fastest because it sets all the parameters with the least run-time
processing.

#### Typed params

In the case, the parameter type has to be explicitly stated, there's a wrapper class -
`Parameter` that will help provide explicit type information.

```kotlin
val param = Parameter(param, String::class.java)
sqlQuery("""select id, name 
    from members 
    where ? is null or ? = name""", 
    param, param)
```

or also with the helper function `param`

```kotlin
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

```kotlin
session.forEach(sqlQuery("select id from members")) { row ->
  // working with large data set
})
```

As an alternative when you need to modify a small amount of values or columns, and then pass the
results as JSON to the front-end, you can convert the result set to JSON object or array and
modify the data in place.

This library uses the [`boxed-json`](https://github.com/vsch/boxed-json) library's
`MutableJsObject` and `MutableJsArray` which allow modifications to the `JsonValue`s without
having to copy the object.

#### Transaction

`Session` object provides transaction block. Transactions are not automatically committed
however any uncaught exceptions will cause the transaction to be automatically rolled back.

The `Transaction` instance is a session with added `java.sql.Connection` transaction methods for
convenience.

```kotlin
session.transaction { tx ->
  // begin
  tx.update(sqlQuery("insert into members (name,  created_at) values (?, ?)", "Alice", Date()))
  tx.commit() // commit must be explicitly specified
}

session.transaction { tx ->
  // begin
  tx.update(sqlQuery("update members set name = ? where id = ?", "Chris", 1))
  throw RuntimeException() // rollback
}
```

#### Models

A base `Model` class can be used to define models which know how to set their properties from a
`Row` result set row, set properties from other Models, understand `auto`, `key` and `default`
properties and can generate INSERT, UPDATE, DELETE queries for a model instance with validation
of required fields and minimal required arguments.

* Define model's properties by using `by prop`. The nullability of the property type dictates
  whether the property can be omitted or set to null
* Key properties by: `by prop.key`, `by key`. These will be used for `WHERE` list for `UPDATE`
  or `DELETE` query generation
* Auto generated (not updatable) properties by: `by prop.auto`, `by auto`
* Auto generated Key (not updatable) properties by: `by prop.key.auto`, `by prop.autoKey`, `by auto.key`, `by
  key.auto` or `by autoKey`
* Properties with default values by: `by prop.default`, `by default`. These won't raise an
  exception for `INSERT` query generation if they are missing from the model's set properties.

By default models allow public setters on properties marked `auto` or `autoKey`, to add a
validation forcing all `auto` properties to have no `set` method or have `private set` pass
`true` for `allowPublicAuto` second parameter to model constructor.

```kotlin
class ValidModel() : Model<ValidModel>(tableName) {
    var processId: Long? by model.auto.key; private set
    var title: String by model
    var version: String by model
    var createdAt: String? by model.auto; private set

    companion object {
        val fromRow: (Row) -> ValidModel = { row ->
            ValidModel().load(row)
        }
        
        val tableName = "tableName"
    }
}

fun useModel() {
    using(session(HikariCP.default())) { session ->
        val modelList = session.list("", ValidModel.fromRow)
        
        val model = ValidModel()
        model.title = "title text"
        model.version = "V1.0"
        
        session.execute(model.insertQuery)
    }
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

Convenience methods that process result sets which use an extractor which take an ResultSet Row
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

#### Configuring SQL Language Injections

You can manually add `@Language("SQL")` annotation to strings or add a language injection
configuration:

The places patterns text is:

```
+ kotlinParameter().ofFunction(0, kotlinFunction().withName("sqlCall").definedInPackage("com.vladsch.kotlin.jdbc"))
+ kotlinParameter().ofFunction(0, kotlinFunction().withName("sqlQuery").definedInPackage("com.vladsch.kotlin.jdbc"))

```

![Language-Injections](assets/images/Language-Injections.png)

To get full benefit of SQL completions you should also define a database source to the database
against which you are developing (or a local clone of it) and configure the SQL dialect for the
database you are using.

## License

(The MIT License)

Copyright (c) 2018 - Vladimir Schneider  
Copyright (c) 2015 - Kazuhiro Sera
