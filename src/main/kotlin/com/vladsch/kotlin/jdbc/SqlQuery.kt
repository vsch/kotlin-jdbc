package com.vladsch.kotlin.jdbc

import java.sql.PreparedStatement

open class SqlQuery(
    val statement: String,
    params: List<Any?> = listOf(),
    inputParams: Map<String, Any?> = mapOf()
) {

    val params = ArrayList(params)
    val inputParams = HashMap(inputParams)
    val replacementMap: Map<String, List<Int>> = extractNamedParamsIndexed(statement)
    val cleanStatement: String = replaceNamedParams(statement)

    private fun extractNamedParamsIndexed(stmt: String): Map<String, List<Int>> {
        return regex.findAll(stmt).mapIndexed { index, group ->
            Pair(group, index)
        }.groupBy({ it.first.value.substring(1) }, { it.second })
    }

    private fun replaceNamedParams(stmt: String): String {
        return regex.replace(stmt, "?")
    }

    fun populateParams(stmt: PreparedStatement) {
        if (replacementMap.isNotEmpty()) {
            replacementMap.forEach { paramName, occurrences ->
                occurrences.forEach {
                    stmt.setTypedParam(it + 1, inputParams[paramName].param())
                }
            }
        } else {
            params.forEachIndexed { index, value ->
                stmt.setTypedParam(index + 1, value.param())
            }
        }
    }

    open fun params(vararg param: Any?): SqlQuery {
        params.addAll(param)
        return this
    }

    open fun inParams(params:Map<String,Any?>): SqlQuery {
        inputParams.putAll(params)
        return this
    }

    open fun inParams(vararg params:Pair<String,Any?>): SqlQuery {
        inputParams.putAll(params)
        return this
    }

    override fun toString(): String {
        return "SqlQuery(statement='$statement', params=$params, inputParams=$inputParams, replacementMap=$replacementMap, cleanStatement='$cleanStatement')"
    }

    companion object {
        private val regex = Regex(""":\w+""")
    }
}
