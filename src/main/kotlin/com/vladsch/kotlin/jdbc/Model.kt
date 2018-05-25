package com.vladsch.kotlin.jdbc

import javax.json.JsonObject
import kotlin.reflect.KProperty

@Suppress("MemberVisibilityCanBePrivate")
abstract class Model<T : Model<T>>(val sqlTable: String, allowSetAuto: Boolean = true) {
    protected val model = ModelProperties<T>(this::class.simpleName ?: "<unknown>", allowSetAuto)

    internal val _model get() = model

    fun load(rs: Row): T {
        model.load(rs)
        @Suppress("UNCHECKED_CAST")
        return this as T
    }

    fun load(json: JsonObject): T {
        model.load(json)
        @Suppress("UNCHECKED_CAST")
        return this as T
    }

    fun load(other: Model<*>): T {
        model.load(other)
        @Suppress("UNCHECKED_CAST")
        return this as T
    }

    fun toJson() = model.toJsonObject()

    val insertQuery: SqlQuery get() = model.sqlInsertQuery(sqlTable)
    val deleteQuery: SqlQuery get() = model.sqlDeleteQuery(sqlTable)
    val updateQuery: SqlQuery get() = model.sqlUpdateQuery(sqlTable)
    val selectQuery: SqlQuery get() = model.sqlSelectQuery(sqlTable)
    val selectSql: String get() = model.sqlSelectTable(sqlTable)

    fun insert(session: Session) {
        session.updateGetKeys(insertQuery) {
            model.loadKeys(it)
        }
    }

    fun insertIgnoreKeys(session: Session) {
        session.update(insertQuery)
    }

    fun select(session: Session) {
        session.first(selectQuery) {
            model.load(it)
        }
    }

    fun insertReload(session: Session) {
        insert(session)
        select(session)
    }

    fun clearAutoKeys() {
        model.clearAutoKeys()
    }

    fun delete(session: Session) {
        session.execute(deleteQuery)
        clearAutoKeys()
    }

    fun update(session: Session) {
        if (session.execute(updateQuery)) {
            snapshot()
        }
    }

    fun updateReload(session: Session) {
        update(session)
        select(session)
    }

    fun deleteKeepAutoKeys(session: Session) {
        delete(session)
    }

    /**
     * set property value directly in the model property map, by passing model properties
     *
     * Can be used to set non-writable properties
     */
    protected fun <V> setProperty(prop: KProperty<*>, value: V) {
        @Suppress("UNCHECKED_CAST")
        model.setProperty(this as T, prop, value)
    }

    fun snapshot() {
        model.snapshot()
    }

    fun isDirty(): Boolean {
        return model.isModified()
    }

    fun isDirty(property: KProperty<*>): Boolean {
        return model.isModified(property)
    }

    fun appendKeys(appendable: Appendable, params: ArrayList<Any?>, delimiter: String = " AND ", sep: String = ""): String {
        return model.appendKeys(appendable, params, delimiter, sep)
    }

    fun forEachKey(consumer: (prop: KProperty<*>, propType: PropertyType, value: Any?) -> Unit) {
        model.forEachKey(consumer)
    }

    fun forEachProp(consumer: (prop: KProperty<*>, propType: PropertyType, value: Any?) -> Unit) {
        model.forEachProp(consumer)
    }

    override fun toString(): String {
        val sb = StringBuilder()
        var sep = ""
        sb.append(model.name).append("(")
        forEachProp { prop, propType, value ->
            if (value !== Unit) {
                sb.append(sep)
                if (propType.isKey) sb.append('*')
                sb.append(prop.name).append("=").append(value)
                sep = ", "
            }
        }
        sb.append(")")
        return sb.toString()
    }
}
