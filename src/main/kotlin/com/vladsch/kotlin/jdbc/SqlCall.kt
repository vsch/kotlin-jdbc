package com.vladsch.kotlin.jdbc

import java.sql.CallableStatement
import java.sql.PreparedStatement

class SqlCall(
    statement: String,
    params: List<Any?> = listOf(),
    namedParams: Map<String, Any?> = mapOf()
) : SqlQueryBase<SqlCall>(statement, params, namedParams) {

    @Deprecated(message = "Use directional parameter construction with to/inTo, outTo, inOutTo infix functions")
    constructor(
        statement: String,
        params: List<Any?> = listOf(),
        inputParams: Map<String, Any?> = mapOf(),
        outputParams: Map<String, Any> = mapOf()
    ) : this(statement, params, inputParams.asParamMap(InOut.IN)) {
        params(outputParams.asParamMap(InOut.OUT))
    }

    override fun populateNamedParams(stmt: PreparedStatement) {
        super.populateNamedParams(stmt)

        stmt as CallableStatement? ?: return

        forEachNamedParam(InOut.OUT) { paramName, param, occurrences ->
            require(occurrences.size == 1) { "Output parameter $paramName should have exactly 1 occurrence, got ${occurrences.size}" }
            stmt.registerOutParameter(occurrences.first() + 1, param.sqlType())
        }
    }

    private fun outputParamIndices(stmt: CallableStatement): Map<String, Int> {
        val params = HashMap<String, Int>()
        forEachNamedParam(InOut.OUT) { paramName, _, occurrences ->
            require(occurrences.size == 1) { "Output parameter $paramName should have exactly 1 occurrence, got ${occurrences.size}" }
            params[paramName] = occurrences.first() + 1
        }
        return params
    }

    fun handleResults(stmt: CallableStatement, stmtProc: (results: SqlCallResults) -> Unit) {
        val results = stmt.execute();
        val outputParams = outputParamIndices(stmt as CallableStatement)
        val callableOutParams = SqlCallResults(stmt, results, outputParams)
        stmtProc.invoke(callableOutParams)
    }

    @Deprecated(message = "Use Session.executeCall which handles out parameters")
    fun outParamIndex(paramName: String): Int {
        require(outputParams.containsKey(paramName)) { "Parameter $paramName is not an outParam" }
        require(replacementMap[paramName]?.size == 1) { "Output parameter $paramName should have exactly 1 occurrence, got ${replacementMap[paramName]?.size}" }
        return replacementMap[paramName]?.first()!! + 1
    }

    @Deprecated(message = "Use directional parameter construction with to/inTo, outTo, inOutTo infix functions", replaceWith = ReplaceWith("params"))
    fun inOutParams(params: Map<String, Any?>): SqlCall {
        return params(params.asParamMap(InOut.IN_OUT))
    }

    @Deprecated(message = "Use directional parameter construction with to/inTo, outTo, inOutTo infix functions", replaceWith = ReplaceWith("params"))
    fun inOutParams(vararg params: Pair<String, Any?>): SqlCall {
        return params(params.asParamMap(InOut.IN_OUT))
    }

    @Deprecated(message = "Use directional parameter construction with to/inTo, outTo, inOutTo infix functions", replaceWith = ReplaceWith("params"))
    fun outParams(params: Map<String, Any?>): SqlCall {
        return params(params.asParamMap(InOut.OUT))
    }

    @Deprecated(message = "Use directional parameter construction with to/inTo, outTo, inOutTo infix functions", replaceWith = ReplaceWith("params"))
    fun outParams(vararg params: Pair<String, Any?>): SqlCall {
        return params(params.asParamMap(InOut.OUT))
    }

    override fun toString(): String {
        return "SqlCall(statement='$statement', params=${params.map { it.value }}, namedParams=${namedParams.map { it.key to it.value.value }}, replacementMap=$replacementMap, cleanStatement='$cleanStatement')"
    }
}
