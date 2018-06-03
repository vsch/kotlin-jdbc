package com.vladsch.kotlin.jdbc

fun describe(description: String, tests: () -> Unit) {
    println(description)

    tests()
}

fun withQueries(vararg stmts: String, assertions: (SqlQuery) -> Unit) {
    stmts.forEach {
        val query = sqlQuery(it)

        assertions(query)
    }
}

fun String.normalizeSpaces(): String {
    return Regex("[ \\t]+").replace(this, " ")
}
