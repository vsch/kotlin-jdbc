package com.vladsch.kotlin.jdbc

import java.sql.PreparedStatement

class SqlQuery(
    statement: String,
    params: List<Any?> = listOf(),
    namedParams: Map<String, Any?> = mapOf()

) : SqlQueryBase<SqlQuery>(statement, params, namedParams) {

    override fun toString(): String {
        return "SqlQuery(statement='$statement', params=${params.map { it.value }}, namedParams=${namedParams.map { it.key to it.value.value }}, replacementMap=$replacementMap, cleanStatement='$cleanStatement')"
    }
}
