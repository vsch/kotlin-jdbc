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

    override fun params(vararg param: Any?): SqlCall {
        params.add(param)
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
}
