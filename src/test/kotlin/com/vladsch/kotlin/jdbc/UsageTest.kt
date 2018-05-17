package com.vladsch.kotlin.jdbc

import org.junit.Test
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.ZonedDateTime
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class UsageTest {

    data class Member(
        val id: Int,
        val name: String?,
        val createdAt: ZonedDateTime)

    val toMember: (Row) -> Member = { row ->
        Member(row.int("id"), row.stringOrNull("name"), row.zonedDateTime("created_at"))
    }

    val insert = "insert into members (name, created_at) values (?, ?)"

    fun borrowConnection(): java.sql.Connection {
        return DriverManager.getConnection("jdbc:h2:mem:hello", "user", "pass")
    }

    val driverName = "org.h2.Driver"

    @Test
    fun sessionUsage() {
        using(borrowConnection()) { conn ->

            val session = Session(Connection(conn, driverName))

            session.execute(sqlQuery("drop table members if exists"))
            session.execute(sqlQuery("""
create table members (
  id serial not null primary key,
  name varchar(64),
  created_at timestamp not null
)
        """))
            session.update(sqlQuery(insert, "Alice", Date()))
            session.update(sqlQuery(insert, "Bob", Date()))

            val ids: List<Int> = session.list(sqlQuery("select id from members")) { row -> row.int("id") }
            assertEquals(2, ids.size)

            val members: List<Member> = session.list(sqlQuery("select id, name, created_at from members"), toMember)
            assertEquals(2, members.size)

            var count = 0
            session.forEach(sqlQuery("select id from members")) { row ->
                count++
                assertNotNull(row.int("id"))
            }
            assertEquals(2, count)

            val nameQuery = "select id, name, created_at from members where name = ?"
            val alice: Member? = session.first(sqlQuery(nameQuery, "Alice"), toMember)
            assertNotNull(alice)

            val bob: Member? = session.first(sqlQuery(nameQuery, "Bob"), toMember)
            assertNotNull(bob)

            val chris: Member? = session.first(sqlQuery(nameQuery, "Chris"), toMember)
            assertNull(chris)
        }
    }

    @Test
    fun addNewWithId() {
        using(borrowConnection()) { conn ->
            val session = Session(Connection(conn, driverName))
            session.execute(sqlQuery("drop table members if exists"))
            session.execute(sqlQuery("""
create table members (
  id serial not null primary key,
  name varchar(64),
  created_at timestamp not null
)
        """))

            // session usage example
            val createdID = session.updateGetId(sqlQuery(insert, "Fred", Date()))
            assertEquals(1, createdID)

            //action usage example
            val createdID2 = session.updateGetId(sqlQuery(insert, "Jane", Date()))
            assertEquals(2, createdID2)
        }
    }

    @Test
    fun actionUsage() {
        using(borrowConnection()) { conn ->

            val session = Session(Connection(conn, driverName))

            session.execute(sqlQuery("drop table members if exists"))
            session.execute(sqlQuery("""
create table members (
  id serial not null primary key,
  name varchar(64),
  created_at timestamp not null
)
        """))

            session.execute(sqlQuery(insert, "Alice", Date()))
            session.execute(sqlQuery(insert, "Bob", Date()))

            val ids: List<Int> = session.list(sqlQuery("select id from members")) { row -> row.int("id") }
            assertEquals(2, ids.size)

            val members: List<Member> = session.list(sqlQuery("select id, name, created_at from members"), toMember)
            assertEquals(2, members.size)

            var count = 0
            session.forEach(sqlQuery("select id from members")) { row ->
                count++
                assertNotNull(row.int("id"))
            }
            assertEquals(2, count)

            val nameQuery = "select id, name, created_at from members where name = ?"
            val alice: Member? = session.first(sqlQuery(nameQuery, "Alice"), toMember)
            assertNotNull(alice)

            val bob: Member? = session.first(sqlQuery(nameQuery, "Bob"), toMember)
            assertNotNull(bob)

            val chris: Member? = session.first(sqlQuery(nameQuery, "Chris"), toMember)
            assertNull(chris)
        }
    }

    @Test
    fun transactionUsage() {
        using(borrowConnection()) { conn ->

            val idsQuery = sqlQuery("select id from members")

            val session = Session(Connection(conn, driverName))

            session.execute(sqlQuery("drop table members if exists"))
            session.execute(sqlQuery("""
create table members (
  id serial not null primary key,
  name varchar(64),
  created_at timestamp not null
)
        """))

            session.execute(sqlQuery(insert, "Alice", Date()))
            session.transaction { tx ->
                tx.update(sqlQuery(insert, "Bob", Date()))
                tx.commit()
            }
            assertEquals(2, session.count(idsQuery))

            try {
                session.transaction { tx ->
                    tx.update(sqlQuery(insert, "Chris", Date()))
                    assertEquals(3, tx.count(idsQuery))
                    throw RuntimeException()
                }
            } catch (e: RuntimeException) {
            }
            assertEquals(2, session.count(idsQuery))

            try {
                session.transaction { tx ->
                    tx.update(sqlQuery(insert, "Chris", Date()))
                    assertEquals(3, tx.count(idsQuery))
                    tx.rollback()
                }
            } catch (e: RuntimeException) {
            }
            assertEquals(2, session.count(idsQuery))
        }
    }

    @Test
    fun HikariCPUsage() {
        HikariCP.default("jdbc:h2:mem:hello", "user", "pass")

        using(session(HikariCP.dataSource())) { session ->
            session.execute(sqlQuery("drop table members if exists"))
            session.execute(sqlQuery("""
create table members (
  id serial not null primary key,
  name varchar(64),
  created_at timestamp not null
)
        """))

            listOf("Alice", "Bob").forEach { name ->
                session.update(sqlQuery(insert, name, Date()))
            }
            val ids: List<Int> = session.list(sqlQuery("select id from members")) { row -> row.int("id") }
            assertEquals(2, ids.size)
        }
    }

    @Test
    fun stmtParamPopulation() {
        withPreparedStmt(sqlQuery("""SELECT * FROM dual t
            WHERE (:param1 IS NULL OR :param2 = :param2)
            AND (:param2 IS NULL OR :param1 = :param3)
            AND (:param3 IS NULL OR :param3 = :param1)""",
            inputParams = mapOf("param1" to "1",
                "param2" to 2,
                "param3" to true))
        ) { preparedStmt ->
            assertEquals("""SELECT * FROM dual t
            WHERE (? IS NULL OR ? = ?) AND (? IS NULL OR ? = ?) AND (? IS NULL OR ? = ?)
            {1: '1', 2: 2, 3: 2, 4: 2, 5: '1', 6: TRUE, 7: TRUE, 8: TRUE, 9: '1'}""".normalizeSpaces(),
                preparedStmt.toString().extractQueryFromPreparedStmt())
        }

        withPreparedStmt(sqlQuery("""SELECT * FROM dual t WHERE (:param1 IS NULL OR :param2 = :param2)""",
            inputParams = mapOf("param2" to 2))
        ) { preparedStmt ->
            assertEquals("""SELECT * FROM dual t WHERE (? IS NULL OR ? = ?)
            {1: NULL, 2: 2, 3: 2}""".normalizeSpaces(),
                preparedStmt.toString().extractQueryFromPreparedStmt())
        }
    }

    @Test
    fun nullParams() {
        withPreparedStmt(sqlQuery("SELECT * FROM dual t WHERE ? IS NULL", null)) { preparedStmt ->
            assertEquals("SELECT * FROM dual t WHERE ? IS NULL {1: NULL}",
                preparedStmt.toString().extractQueryFromPreparedStmt())
        }

        withPreparedStmt(sqlQuery("SELECT * FROM dual t WHERE ? = 1 AND ? IS NULL", 1, null)) { preparedStmt ->
            assertEquals("SELECT * FROM dual t WHERE ? = 1 AND ? IS NULL {1: 1, 2: NULL}",
                preparedStmt.toString().extractQueryFromPreparedStmt())
        }

        withPreparedStmt(sqlQuery("SELECT * FROM dual t WHERE ? = 1 AND ? IS NULL AND ? = 3", 1, null, 3)) { preparedStmt ->
            assertEquals("SELECT * FROM dual t WHERE ? = 1 AND ? IS NULL AND ? = 3 {1: 1, 2: NULL, 3: 3}",
                preparedStmt.toString().extractQueryFromPreparedStmt())
        }
    }

    @Test
    fun nullParamsJdbcHandling() {
        // this test could fail for PostgreSQL
        using(borrowConnection()) { conn ->
            val session = Session(Connection(conn, driverName))

            session.execute(sqlQuery("drop table if exists members"))
            session.execute(sqlQuery("""
create table members (
  id serial not null primary key
)
        """))
            session.execute(sqlQuery("insert into members(id) values (1)"))

            describe("typed param with value") {
                assertEquals(1, session.first(sqlQuery("select 1 from members where ? = id", 1)) { row -> row.int(1) })
            }

            describe("typed null params") {
                assertEquals(1, session.first(sqlQuery("select 1 from members where ? is null", null.param<String>())) { row -> row.int(1) })
            }

            describe("typed null comparison") {
                assertEquals(1,
                    session.first(sqlQuery("select 1 from members where ? is null or ? = now()",
                        null.param<String>(),
                        null.param<Timestamp>())) { row -> row.int(1) }
                )
            }

            describe("select null") {
                val param: String? = null
                assertNull(session.first(sqlQuery("select ? from members", Parameter(param, String::class.java))) { row -> row.stringOrNull(1) })
            }

            session.execute(sqlQuery("drop table if exists members"))
        }
    }

    fun withPreparedStmt(query: SqlQuery, closure: (PreparedStatement) -> Unit) {
        using(borrowConnection()) { conn ->
            val session = Session(Connection(conn, driverName))

            val preparedStmt = session.prepare(query)

            closure(preparedStmt)
        }
    }

    fun String.extractQueryFromPreparedStmt(): String {
        return this.replace(Regex("^.*?: "), "").normalizeSpaces()
    }
}
