package com.vladsch.kotlin.jdbc

import javax.json.JsonObject
import kotlin.reflect.KProperty

@Suppress("MemberVisibilityCanBePrivate", "UNCHECKED_CAST", "PropertyName")
abstract class Model<M : Model<M, D>, D>(session: Session?, sqlTable: String, dbCase: Boolean, allowSetAuto: Boolean = true, quote: String? = null) {
    internal val _db = ModelProperties<Model<M, D>>(session ?: session(), sqlTable, dbCase, allowSetAuto, quote)
    protected val db: ModelPropertyProvider<Model<M, D>> get() = _db

    fun load(rs: Row): M {
        _db.load(rs)
        return this as M
    }

    fun load(json: JsonObject): M {
        _db.load(json)
        return this as M
    }

    fun load(other: Model<*, *>): M {
        _db.load(other)
        return this as M
    }

    val _session: Session get() = _db.session
    val _quote: String get() = _db.quote

    fun toJson() = _db.toJsonObject()

    val insertQuery: SqlQuery get() = _db.sqlInsertQuery()
    val deleteQuery: SqlQuery get() = _db.sqlDeleteQuery()
    val updateQuery: SqlQuery get() = _db.sqlUpdateQuery()
    val selectQuery: SqlQuery get() = _db.sqlSelectQuery()
    val selectSql: String get() = _db.sqlSelectTable()

    fun Appendable.appendQuoted(id: kotlin.String): Appendable {
        return _db.appendQuoted(this, id)
    }

    fun Appendable.appendSqlSelectTable(): Appendable {
        return _db.appendSelectSql(this)
    }

    fun insert() = _db.insert()
    fun insertIgnoreKeys() = _db.insertIgnoreKeys()
    fun select() = _db.select()
    fun insertReload() = _db.insertReload()
    fun clearAutoKeys() = _db.clearAutoKeys()
    fun delete() = _db.delete()
    fun update() = _db.update()
    fun updateReload() = _db.updateReload()
    fun deleteKeepAutoKeys() = _db.deleteKeepAutoKeys()
    fun snapshot() = _db.snapshot()
    fun isDirty(): Boolean = _db.isModified()
    fun isDirty(property: KProperty<*>): Boolean = _db.isModified(property)
    fun appendKeys(appendable: Appendable, params: ArrayList<Any?>, delimiter: String = " AND ", sep: String = ""): String =
            _db.appendKeys(appendable, params, delimiter, sep, null)

    fun forEachKey(consumer: (prop: KProperty<*>, propType: PropertyType, value: Any?) -> Unit) = _db.forEachKey(consumer)

    fun forEachProp(consumer: (prop: KProperty<*>, propType: PropertyType, value: Any?) -> Unit) = _db.forEachProp(consumer)

    fun forEachKey(consumer: (prop: KProperty<*>, propType: PropertyType, columnName: String, value: Any?) -> Unit) =
            _db.forEachKey(consumer)

    fun forEachProp(consumer: (prop: KProperty<*>, propType: PropertyType, columnName: String, value: Any?) -> Unit) =
            _db.forEachProp(consumer)

    /**
     * set property value directly in the _model property map, by passing _model properties
     *
     * Can be used to set non-writable properties
     */
    protected fun <V> setProperty(prop: KProperty<*>, value: V) {
        @Suppress("UNCHECKED_CAST")
        _db.setProperty(this, prop, value)
    }

    fun quoteIdentifier(id: String): String = _db.appendQuoted(StringBuilder(), id).toString()

    fun appendSelectSql(out: Appendable, alias: String? = null): Appendable = _db.appendSelectSql(out, alias)

    fun appendListQuery(out: Appendable, params: Array<out Pair<String, Any?>>, alias: String? = null) = _db.appendListQuery(out, params, alias)

    fun appendListQuery(out: Appendable, params: Map<String, Any?>, alias: String? = null) = _db.appendListQuery(out, params, alias)

    fun listQuery(params: Map<String, Any?>, alias: String? = null) = _db.listQuery(params, alias)

    fun listQuery(vararg params: Pair<String, Any?>, alias: String? = null) = _db.listQuery(params, alias)

    fun listQuery(whereClause: String, params: Map<String, Any?>, alias: String? = null) = _db.listQuery(whereClause, params, alias)

    fun listData() = _db.listData(toData)

    fun listData(whereClause: String) = _db.listData(whereClause, toData)

    fun listData(sqlQuery: SqlQuery) = _db.session.list(sqlQuery, toData)

    fun listData(whereClause: String, params: Map<String, Any?>) = _db.listData(whereClause, params, toData)

    fun listModel() = _db.listModel(toModel)

    fun listModel(whereClause: String) = _db.listModel(whereClause, toModel)

    fun listModel(sqlQuery: SqlQuery) = _db.session.list(sqlQuery, toModel)

    fun listModel(whereClause: String, params: Map<String, Any?>) = _db.listModel(whereClause, params, toModel)

    // create a new copy, same params
    abstract operator fun invoke(): M

    // create a data of this
    abstract fun toData(): D

    // instance in this model used to load lists
    private val loader: M by lazy { invoke() }

    val toData: (Row) -> D = {
        loader.load(it).toData()
    }

    val toModel: (Row) -> M = {
        loader.load(it)
    }

    override fun toString(): String {
        val sb = StringBuilder()
        var sep = ""
        sb.append(_db.tableName).append("(")
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
