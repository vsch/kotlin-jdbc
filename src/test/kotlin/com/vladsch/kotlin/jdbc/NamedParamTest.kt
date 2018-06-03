package com.vladsch.kotlin.jdbc

import org.junit.Test
import kotlin.test.assertEquals

class NamedParamTest {

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
                assertEquals(
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
                assertEquals(
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

        describe("should ignore -- commented out params") {
            val query = sqlQuery(
                    """SELECT * FROM table t WHERE (:param1 IS NULL OR t.a = :param2)
                 -- AND (:param2 IS NULL OR t.b = :param3)
                 AND (:param3 IS NULL OR t.c = :param1)"""
            )
                assertEquals(
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
                assertEquals(
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
                assertEquals(
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
                assertEquals(
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
                assertEquals(
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
