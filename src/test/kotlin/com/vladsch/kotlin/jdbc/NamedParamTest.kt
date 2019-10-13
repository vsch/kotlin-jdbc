package com.vladsch.kotlin.jdbc

import org.junit.Test
//import kotlin.test.assertEquals
import org.junit.Assert.assertEquals

class NamedParamTest {
    companion object {
        private val EMPTY_REPLACEMENTS = mapOf<String, List<Int>>()
    }

    @Test
    fun paramExtraction() {
        describe("should extract a single param") {
            withQueries(
                "SELECT * FROM table t WHERE t.a = :param"
            ) { query ->
                assertEquals("""SELECT * FROM table t WHERE t.a = ?""".normalizeSpaces(), query.cleanStatement.normalizeSpaces())
                assertEquals(mapOf("param" to listOf(0)), query.replacementMap)
            }
        }

        describe("should not extract param in string") {
            withQueries(
                "INSERT INTO table VALUES (':param')"
            ) { query ->
                assertEquals("""INSERT INTO table VALUES (':param')""".normalizeSpaces(), query.cleanStatement.normalizeSpaces())
                assertEquals(EMPTY_REPLACEMENTS, query.replacementMap)
            }
        }

        describe("should not extract param in back quotes") {
            withQueries(
                "INSERT INTO `:table` VALUES (':param')"
            ) { query ->
                assertEquals("""INSERT INTO `:table` VALUES (':param')""".normalizeSpaces(), query.cleanStatement.normalizeSpaces())
                assertEquals(EMPTY_REPLACEMENTS, query.replacementMap)
            }
        }

        describe("should not extract param in double quotes") {
            withQueries(
                "INSERT INTO \":table\" VALUES (':param')"
            ) { query ->
                assertEquals("""INSERT INTO ":table" VALUES (':param')""".normalizeSpaces(), query.cleanStatement.normalizeSpaces())
                assertEquals(EMPTY_REPLACEMENTS, query.replacementMap)
            }
        }

        describe("should extract a single param") {
            withQueries(
                """SELECT * FROM table t WHERE t.a =
                :param"""
            ) { query ->
                assertEquals("""SELECT * FROM table t WHERE t.a =
                    ?""".normalizeSpaces(), query.cleanStatement.normalizeSpaces())
                assertEquals(mapOf("param" to listOf(0)), query.replacementMap)
            }
        }

        describe("should extract multiple params") {
            withQueries(
                "SELECT * FROM table t WHERE t.a = :param1 AND t.b = :param2"
            ) { query ->
                assertEquals("SELECT * FROM table t WHERE t.a = ? AND t.b = ?", query.cleanStatement)
                assertEquals(mapOf(
                    "param1" to listOf(0),
                    "param2" to listOf(1)
                ), query.replacementMap)
            }
        }

        describe("should extract multiple repeated params") {
            withQueries(
                """SELECT * FROM table t WHERE (:param1 IS NULL OR t.a = :param2)
                 AND (:param2 IS NULL OR t.b = :param3)
                 AND (:param3 IS NULL OR t.c = :param1)"""
            ) { query ->
                assertEquals("message",
                    """SELECT * FROM table t WHERE (? IS NULL OR t.a = ?)
 AND (? IS NULL OR t.b = ?)
 AND (? IS NULL OR t.c = ?)""".normalizeSpaces(), query.cleanStatement.normalizeSpaces()
                )
                assertEquals(mapOf(
                    "param1" to listOf(0, 5),
                    "param2" to listOf(1, 2),
                    "param3" to listOf(3, 4)
                ), query.replacementMap)
            }
        }

        describe("should extract multiple repeated params") {
            withQueries(
                """SELECT * FROM table t WHERE (:param1 IS NULL OR t.a = :param2)
                 --AND (:param2 IS NULL OR t.b = :param3)
                 AND (:param3 IS NULL OR t.c = :param1)"""
            ) { query ->
                assertEquals("message",
                    """SELECT * FROM table t WHERE (? IS NULL OR t.a = ?)
 --AND (? IS NULL OR t.b = ?)
 AND (? IS NULL OR t.c = ?)""".normalizeSpaces(), query.cleanStatement.normalizeSpaces()
                )
                assertEquals(mapOf(
                    "param1" to listOf(0, 5),
                    "param2" to listOf(1, 2),
                    "param3" to listOf(3, 4)
                ), query.replacementMap)
            }
        }
    }

    @Test
    fun paramListExtraction() {
        describe("should extract a single list param") {
            withQueries(
                """SELECT * FROM table t WHERE t.a IN
                (:param)"""
            ) { query ->
                query.params("param" inTo listOf(0, 1, 2))

                val cleanStatement = query.cleanStatement

                assertEquals("""SELECT * FROM table t WHERE t.a IN
                    (?,?,?)""".normalizeSpaces(), cleanStatement.normalizeSpaces())
                assertEquals(mapOf("param" to listOf(0)), query.replacementMap)
            }
        }

        describe("should extract a single list param after single param") {
            withQueries(
                """SELECT * FROM table t WHERE t.b = :paramB AND t.a IN (:param)""") { query ->
                query.inParams("param" to listOf(0, 1, 2), "paramB" to listOf(10))

                val cleanStatement = query.cleanStatement

                assertEquals("""SELECT * FROM table t WHERE t.b = ? AND t.a IN (?,?,?)""", cleanStatement.normalizeSpaces())
                assertEquals(mapOf("param" to listOf(1), "paramB" to listOf(0)), query.replacementMap)
                val actual = query.getParams()
                assertEquals(listOf<Any?>(10, 0, 1, 2), actual)
            }
        }

        describe("should extract a single list param before single param") {
            withQueries(
                """SELECT * FROM table t WHERE t.a IN (:param) AND t.b = :paramB""") { query ->
                query.inParams("param" to listOf(0, 1, 2), "paramB" to listOf(10))

                val cleanStatement = query.cleanStatement

                assertEquals("""SELECT * FROM table t WHERE t.a IN (?,?,?) AND t.b = ?""", cleanStatement.normalizeSpaces())
                assertEquals(mapOf("param" to listOf(0), "paramB" to listOf(3)), query.replacementMap)
                assertEquals(listOf<Any?>(0, 1, 2, 10), query.getParams())
            }
        }

        describe("should extract multiple repeated list params") {
            withQueries(
                """SELECT * FROM table t WHERE t.a IN (:param) OR t.b IN (:param)""") { query ->
                query.inParams("param" to listOf(0, 1, 2))

                val cleanStatement = query.cleanStatement

                assertEquals("""SELECT * FROM table t WHERE t.a IN (?,?,?) OR t.b IN (?,?,?)""", cleanStatement.normalizeSpaces())
                assertEquals(mapOf("param" to listOf(0, 3)), query.replacementMap)
                assertEquals(listOf<Any?>(0, 1, 2, 0, 1, 2), query.getParams())
            }
        }
    }

    @Test
    fun commentsExtraction() {
        describe("should ignore -- commented out params") {
            val query = sqlQuery(
                """SELECT * FROM table t WHERE (:param1 IS NULL OR t.a = :param2)
                 -- AND (:param2 IS NULL OR t.b = :param3)
                 AND (:param3 IS NULL OR t.c = :param1)"""
            )
            assertEquals("message",
                """SELECT * FROM table t WHERE (? IS NULL OR t.a = ?)
 -- AND (? IS NULL OR t.b = ?)
 AND (? IS NULL OR t.c = ?)""".normalizeSpaces(), query.cleanStatement.normalizeSpaces()
            )
            assertEquals(mapOf(
                "param1" to listOf(0, 3),
                "param2" to listOf(1),
                "param3" to listOf(2)
            ), query.replacementMap)
        }

        describe("should ignore -- commented out params") {
            val query = sqlQuery(
                """-- leading comment
                        SELECT * FROM table t WHERE (:param1 IS NULL OR t.a = :param2)
                 -- AND (:param2 IS NULL OR t.b = :param3)
                 AND (:param3 IS NULL OR t.c = :param1)"""
            )
            assertEquals("message",
                """-- leading comment
 SELECT * FROM table t WHERE (? IS NULL OR t.a = ?)
 -- AND (? IS NULL OR t.b = ?)
 AND (? IS NULL OR t.c = ?)""".normalizeSpaces(), query.cleanStatement.normalizeSpaces()
            )
            assertEquals(mapOf(
                "param1" to listOf(0, 3),
                "param2" to listOf(1),
                "param3" to listOf(2)
            ), query.replacementMap)
        }

        describe("should ignore -- commented out params") {
            val query = sqlQuery(
                """SELECT * FROM table t WHERE (:param1 IS NULL OR t.a = :param2)
-- AND (:param2 IS NULL OR t.b = :param3)
                 AND (:param3 IS NULL OR t.c = :param1)"""
            )
            assertEquals("message",
                """SELECT * FROM table t WHERE (? IS NULL OR t.a = ?)
-- AND (? IS NULL OR t.b = ?)
 AND (? IS NULL OR t.c = ?)""".normalizeSpaces(), query.cleanStatement.normalizeSpaces()
            )
            assertEquals(mapOf(
                "param1" to listOf(0, 3),
                "param2" to listOf(1),
                "param3" to listOf(2)
            ), query.replacementMap)
        }

        describe("should ignore # commented out params") {
            withQueries(
                """SELECT * FROM table t WHERE (:param1 IS NULL OR t.a = :param2)
#AND (:param2 IS NULL OR t.b = :param3)
                 AND (:param3 IS NULL OR t.c = :param1)"""
            ) { query ->
                assertEquals("message",
                    """SELECT * FROM table t WHERE (? IS NULL OR t.a = ?)
#AND (? IS NULL OR t.b = ?)
 AND (? IS NULL OR t.c = ?)""".normalizeSpaces(), query.cleanStatement.normalizeSpaces()
                )
                assertEquals(mapOf(
                    "param1" to listOf(0, 3),
                    "param2" to listOf(1),
                    "param3" to listOf(2)
                ), query.replacementMap)
            }
        }

        describe("should ignore # commented out params") {
            withQueries(
                """SELECT * FROM table t WHERE (:param1 IS NULL OR t.a = :param2)
    #AND (:param2 IS NULL OR t.b = :param3)
                 AND (:param3 IS NULL OR t.c = :param1)"""
            ) { query ->
                assertEquals("message",
                    """SELECT * FROM table t WHERE (? IS NULL OR t.a = ?)
 #AND (? IS NULL OR t.b = ?)
 AND (? IS NULL OR t.c = ?)""".normalizeSpaces(), query.cleanStatement.normalizeSpaces()
                )
                assertEquals(mapOf(
                    "param1" to listOf(0, 3),
                    "param2" to listOf(1),
                    "param3" to listOf(2)
                ), query.replacementMap)
            }
        }
    }
}
