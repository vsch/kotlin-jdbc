package com.vladsch.kotlin.jdbc

import org.jetbrains.annotations.TestOnly
import java.sql.PreparedStatement

open class SqlQuery(
    val statement: String,
    params: List<Any?> = listOf(),
    inputParams: Map<String, Any?> = mapOf()
) {
    protected val params = ArrayList(params)
    protected val inputParams = HashMap(inputParams)
    private var _queryDetails: Details? = null

    val queryDetails: Details get() = finalizeQuery()
    val replacementMap get() = queryDetails.replacementMap
    val cleanStatement get() = queryDetails.cleanStatement

    data class Details(
        val listParamsMap: Map<String, String>,
        val replacementMap: Map<String, List<Int>>,
        val cleanStatement: String,
        val paramCount: Int
    )

    protected fun resetDetails() {
        _queryDetails = null
    }

    private fun finalizeQuery(): Details {
        if (_queryDetails == null) {
            // called when parameters are defined and have request for clean statement or populate params
            val listParamsMap: HashMap<String, String> = HashMap()
            var idxOffset = 0
            val findAll = regex.findAll(statement)
            val replacementMap = findAll.filter { group ->
                if (!group.value.startsWith(":")) {
                    // not a parameter
                    false
                } else {
                    val pos = statement.lastIndexOf('\n', group.range.first)
                    val lineStart = if (pos == -1) 0 else pos + 1;
                    !regexSqlComment.containsMatchIn(statement.subSequence(lineStart, statement.length))
                }
            }.map { group ->
                val paramName = group.value.substring(1);
                val paramValue = inputParams[paramName]
                val pair = Pair(paramName, idxOffset)

                if (paramValue is Collection<*>) {
                    val size = paramValue.size
                    listParamsMap[paramName] = "?,".repeat(size).substring(0, size * 2 - 1)
                    idxOffset += size - 1
                }

                idxOffset++
                pair
            }.groupBy({ it.first }, { it.second })

            val cleanStatement = regex.replace(statement) { matchResult ->
                if (!matchResult.value.startsWith(":")) {
                    // not a parameter, leave as is
                    matchResult.value
                } else {
                    val paramName = matchResult.value.substring(1);
                    listParamsMap[paramName] ?: "?"
                }
            }
            _queryDetails = Details(listParamsMap, replacementMap, cleanStatement, idxOffset)
        }
        return _queryDetails!!
    }

    fun populateParams(stmt: PreparedStatement) {
        if (replacementMap.isNotEmpty()) {
            replacementMap.forEach { paramName, occurrences ->
                populateNamedParam(stmt, paramName, occurrences)
            }
        } else {
            params.forEachIndexed { index, value ->
                stmt.setTypedParam(index + 1, value.param())
            }
        }
    }

    protected open fun populateNamedParam(stmt: PreparedStatement, paramName: String, occurrences: List<Int>) {
        occurrences.forEach {
            val param = inputParams[paramName]
            if (param is Collection<*>) {
                param.forEachIndexed { idx, paramItem ->
                    stmt.setTypedParam(it + idx + 1, paramItem.param())
                }
            } else {
                stmt.setTypedParam(it + 1, param.param())
            }
        }
    }

    @TestOnly
    fun getParams(): List<Any?> {
        return if (replacementMap.isNotEmpty()) {
            val sqlParams = ArrayList<Any?>(queryDetails.paramCount)

            for (i in 0 until  queryDetails.paramCount) {
                sqlParams.add(null)
            }

            replacementMap.forEach { paramName, occurrences ->
                occurrences.forEach {
                    val param = inputParams[paramName]
                    if (param is Collection<*>) {
                        param.forEachIndexed { idx, paramItem ->
                            sqlParams[it + idx] =  paramItem
                        }
                    } else {
                        sqlParams[it] =  param
                    }
                }
            }
            sqlParams
        } else {
            params
        }
    }

    open fun params(vararg params: Any?): SqlQuery {
        this.params.addAll(params)
        this._queryDetails = null
        return this
    }

    open fun paramsArray(params: Array<out Any?>): SqlQuery {
        this.params.addAll(params)
        this._queryDetails = null
        return this
    }

    open fun paramsList(params: Collection<Any?>): SqlQuery {
        this.params.addAll(params)
        this._queryDetails = null
        return this
    }

    open fun inParams(params: Map<String, Any?>): SqlQuery {
        inputParams.putAll(params)
        this._queryDetails = null
        return this
    }

    open fun inParams(vararg params: Pair<String, Any?>): SqlQuery {
        inputParams.putAll(params)
        this._queryDetails = null
        return this
    }

    open fun inParamsArray(params: Array<out Pair<String, Any?>>): SqlQuery {
        inputParams.putAll(params)
        this._queryDetails = null
        return this
    }

    override fun toString(): String {
        return "SqlQuery(statement='$statement', params=$params, inputParams=$inputParams, replacementMap=$replacementMap, cleanStatement='$cleanStatement')"
    }

    companion object {
        // must begin with
        private val regex = Regex(":\\w+|'(?:[^']|'')*'|`(?:[^`])*`|\"(?:[^\"])*\"")
        private val regexSqlComment = Regex("""^\s*(?:--\s|#)""")
    }
}
