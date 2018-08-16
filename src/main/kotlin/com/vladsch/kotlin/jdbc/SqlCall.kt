package com.vladsch.kotlin.jdbc

import java.sql.CallableStatement

class SqlCall(
    statement: String,
    params: List<Any?> = listOf(),
    inputParams: Map<String, Any?> = mapOf(),
    outputParams: Map<String, Any> = mapOf()
) : SqlQuery(statement, params, inputParams) {

    val outputParams = HashMap(outputParams)

    fun populateParams(stmt: CallableStatement) {
        if (replacementMap.isNotEmpty()) {
            replacementMap.forEach { paramName, occurrences ->
                occurrences.forEach {
                    stmt.setTypedParam(it + 1, inputParams[paramName].param())
                }

                if (outputParams.containsKey(paramName)) {
                    // setup out or inout param
                    stmt.registerOutParameter(paramName, outputParams[paramName].param().sqlType())
                }
            }
        } else {
            params.forEachIndexed { index, value ->
                stmt.setTypedParam(index + 1, value.param())
            }
        }
    }

    override fun params(vararg params: Any?): SqlQuery {
        return paramsArray(params)
    }

    override fun paramsArray(params: Array<out Any?>): SqlQuery {
        this.params.addAll(params)
        return this
    }

    override fun paramsList(params: Collection<Any?>): SqlQuery {
        this.params.addAll(params)
        return this
    }

    override fun inParams(params: Map<String, Any?>): SqlCall {
        inputParams.putAll(params)
        return this
    }

    override fun inParams(vararg params: Pair<String, Any?>): SqlCall {
        inputParams.putAll(params)
        return this
    }

    fun inOutParams(params: Map<String, Any?>): SqlCall {
        inputParams.putAll(params)
        outputParams.putAll(params)
        return this
    }

    fun inOutParams(vararg params: Pair<String, Any?>): SqlCall {
        inputParams.putAll(params)
        outputParams.putAll(params)
        return this
    }

    fun outParams(params: Map<String, Any?>): SqlCall {
        outputParams.putAll(params)
        return this
    }

    fun outParams(vararg params: Pair<String, Any?>): SqlCall {
        outputParams.putAll(params)
        return this
    }

    override fun toString(): String {
        return "SqlCall(outputParams=$outputParams) ${super.toString()}"
    }
}
