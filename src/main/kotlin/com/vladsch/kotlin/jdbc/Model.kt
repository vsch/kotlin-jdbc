package com.vladsch.kotlin.jdbc

import javax.json.JsonObject
import kotlin.reflect.KProperty

abstract class Model<T : Model<T>>(val sqlTable: String, allowSetAuto: Boolean = true) {
    internal val modelProperties = ModelProperties<T>(this::class.simpleName ?: "<unknown>", allowSetAuto)
    protected val model get() = modelProperties

    fun load(rs: Row): T {
        modelProperties.load(rs)
        return this as T
    }

    fun load(json: JsonObject): T {
        modelProperties.load(json)
        return this as T
    }

    fun load(other: Model<*>): T {
        modelProperties.load(other)
        return this as T
    }

    val insertQuery: SqlQuery get() = modelProperties.sqlInsertQuery(sqlTable)
    val deleteQuery: SqlQuery get() = modelProperties.sqlDeleteQuery(sqlTable)
    val updateQuery: SqlQuery get() = modelProperties.sqlUpdateQuery(sqlTable)
    val selectQuery: SqlQuery get() = modelProperties.sqlSelectQuery(sqlTable)
    val selectSql: String get() = modelProperties.sqlSelect(sqlTable)

    fun insertSetKeys(session: Session) {
        session.updateGetKeys(insertQuery) {
            modelProperties.loadKeys(it)
        }
    }

    fun insertReload(session: Session) {
        insertSetKeys(session)
        session.first(selectQuery) {
            modelProperties.load(it)
        }
    }

    protected fun <V> setProperty(prop: KProperty<*>, value: V) {
        modelProperties.setProperty(this as T, prop, value)
    }

    fun snapshot() {
        modelProperties.snapshot()
    }

    fun isDirty(): Boolean {
        return modelProperties.isModified()
    }

    fun isDirty(property: KProperty<*>): Boolean {
        return modelProperties.isModified(property)
    }

    override fun toString(): String {
        val sb = StringBuilder()
        var sep = ""
        sb.append(modelProperties.modelName).append("(")
        modelProperties.forEach { prop, propType, value ->
            if (value !== Unit) {
                sb.append(sep).append(prop.name).append("=").append(value)
                sep = ", "
            }
        }
        sb.append(")")
        return sb.toString()
    }
}
