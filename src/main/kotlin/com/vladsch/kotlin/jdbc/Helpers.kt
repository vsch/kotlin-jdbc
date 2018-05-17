package com.vladsch.kotlin.jdbc

import com.vladsch.boxed.json.BoxedJsValue
import com.vladsch.boxed.json.MutableJsArray
import com.vladsch.boxed.json.MutableJsObject
import org.joda.time.LocalDateTime
import java.io.InputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.net.URL
import java.sql.*
import java.time.*
import java.util.Date
import javax.json.JsonObject
import javax.json.JsonValue
import javax.sql.DataSource

inline fun <reified T> PreparedStatement.setTypedParam(idx: Int, param: Parameter<T>) {
    if (param.value == null) {
        this.setNull(idx, param.sqlType())
    } else {
        setParam(idx, param.value)
    }
}

fun PreparedStatement.setParam(idx: Int, v: Any?) {
    if (v == null) {
        this.setObject(idx, null)
    } else {
        when (v) {
            is String -> this.setString(idx, v)
            is Byte -> this.setByte(idx, v)
            is Boolean -> this.setBoolean(idx, v)
            is Int -> this.setInt(idx, v)
            is Long -> this.setLong(idx, v)
            is Short -> this.setShort(idx, v)
            is Double -> this.setDouble(idx, v)
            is Float -> this.setFloat(idx, v)
            is ZonedDateTime -> this.setTimestamp(idx, Timestamp(Date.from(v.toInstant()).time))
            is OffsetDateTime -> this.setTimestamp(idx, Timestamp(Date.from(v.toInstant()).time))
            is Instant -> this.setTimestamp(idx, Timestamp(Date.from(v).time))
            is java.time.LocalDateTime -> this.setTimestamp(idx, Timestamp(LocalDateTime.parse(v.toString()).toDate().time))
            is LocalDate -> this.setDate(idx, java.sql.Date(org.joda.time.LocalDate.parse(v.toString()).toDate().time))
            is LocalTime -> this.setTime(idx, java.sql.Time(org.joda.time.LocalTime.parse(v.toString()).toDateTimeToday().millis))
            is org.joda.time.DateTime -> this.setTimestamp(idx, Timestamp(v.toDate().time))
            is org.joda.time.LocalDateTime -> this.setTimestamp(idx, Timestamp(v.toDate().time))
            is org.joda.time.LocalDate -> this.setDate(idx, java.sql.Date(v.toDate().time))
            is org.joda.time.LocalTime -> this.setTime(idx, java.sql.Time(v.toDateTimeToday().millis))
            is java.util.Date -> this.setTimestamp(idx, Timestamp(v.time))
            is java.sql.Timestamp -> this.setTimestamp(idx, v)
            is java.sql.Time -> this.setTime(idx, v)
            is java.sql.Date -> this.setTimestamp(idx, Timestamp(v.time))
            is java.sql.SQLXML -> this.setSQLXML(idx, v)
            is ByteArray -> this.setBytes(idx, v)
            is InputStream -> this.setBinaryStream(idx, v)
            is BigDecimal -> this.setBigDecimal(idx, v)
            is java.sql.Array -> this.setArray(idx, v)
            is URL -> this.setURL(idx, v)
            else -> this.setObject(idx, v)
        }
    }
}

open class Parameter<out T>(val value: T?, val type: Class<out T>)

fun <T> Parameter<T>.sqlType() = when (type) {
    String::class.java, URL::class.java -> Types.VARCHAR
    Int::class.java, Long::class.java, Short::class.java,
    Byte::class.java, BigInteger::class.java -> Types.NUMERIC
    Double::class.java, BigDecimal::class.java -> Types.DOUBLE
    Float::class.java -> Types.FLOAT
    java.sql.Date::class.java, java.util.Date::class.java, ZonedDateTime::class.java, OffsetDateTime::class.java,
    Instant::class.java, java.time.LocalDateTime::class.java, org.joda.time.DateTime::class.java, org.joda.time.LocalDateTime::class.java,
    java.sql.Timestamp::class.java -> Types.TIMESTAMP
    java.sql.Time::class.java, LocalTime::class.java -> Types.TIME
    LocalDate::class.java -> Types.DATE
    else -> Types.OTHER
}

inline fun <reified T> T?.param(): Parameter<T> = when (this) {
    is Parameter<*> -> Parameter(this.value as T?, this.type as Class<T>)
    else -> Parameter(this, T::class.java)
}

fun sqlQuery(statement: String, vararg params: Any?): SqlQuery {
    return SqlQuery(statement, params = params.toList())
}

fun sqlQuery(statement: String, inputParams: Map<String, Any?>): SqlQuery {
    return SqlQuery(statement, inputParams = inputParams)
}

fun sqlCall(statement: String, vararg params: Any?): SqlCall {
    return SqlCall(statement, params = params.toList())
}

fun sqlCall(statement: String, inputParams: Map<String, Any?>, outputParams: Map<String, Any>): SqlCall {
    return SqlCall(statement, inputParams = inputParams, outputParams = outputParams)
}

fun session(url: String, user: String, password: String, returnGeneratedKey: Boolean = false): Session {
    val conn = DriverManager.getConnection(url, user, password)
    return Session(Connection(conn), returnGeneratedKey)
}

fun session(dataSource: DataSource, returnGeneratedKey: Boolean = false): Session {
    return Session(Connection(dataSource.connection), returnGeneratedKey)
}

fun <A : AutoCloseable, R> using(closeable: A?, f: (A) -> R): R {
    try {
        if (closeable != null) {
            return f(closeable)
        } else {
            throw IllegalStateException("Closeable resource is unexpectedly null.")
        }
    } finally {
        closeable?.close()
    }
}

fun <V : Any> putNullable(jsonObject: MutableJsObject, name: String, v: V?, action: (V) -> Unit) {
    if (v == null) {
        jsonObject.put(name, JsonValue.NULL)
    } else {
        action.invoke(v)
    }
}

fun addJsonValue(jsonObject: MutableJsObject, column: Int, rs: ResultSet) {
    val metaData = rs.metaData
    val columnType = metaData.getColumnType(column)
    val columnName = metaData.getColumnLabel(column)
    when (columnType) {
        Types.BIT, Types.TINYINT, Types.SMALLINT, Types.INTEGER -> putNullable(jsonObject, columnName, rs.getInt(column)) { jsonObject.put(columnName, it) }
        Types.BIGINT -> putNullable(jsonObject, columnName, rs.getLong(column)) { jsonObject.put(columnName, it) }
        Types.FLOAT -> putNullable(jsonObject, columnName, rs.getDouble(column)) { jsonObject.put(columnName, it) }
        Types.REAL -> putNullable(jsonObject, columnName, rs.getDouble(column)) { jsonObject.put(columnName, it) }
        Types.DOUBLE -> putNullable(jsonObject, columnName, rs.getDouble(column)) { jsonObject.put(columnName, it) }
        Types.NUMERIC -> putNullable(jsonObject, columnName, rs.getDouble(column)) { jsonObject.put(columnName, it) }
        Types.DECIMAL -> putNullable(jsonObject, columnName, rs.getBigDecimal(column)) { jsonObject.put(columnName, it) }
        Types.CHAR -> putNullable(jsonObject, columnName, rs.getString(column)) { jsonObject.put(columnName, it) }
        Types.VARCHAR -> putNullable(jsonObject, columnName, rs.getString(column)) { jsonObject.put(columnName, it) }
        Types.LONGVARCHAR -> putNullable(jsonObject, columnName, rs.getString(column)) { jsonObject.put(columnName, it) }
        Types.DATE -> putNullable(jsonObject, columnName, rs.getDate(column)) { jsonObject.put(columnName, it.toString()) }
        Types.TIME -> putNullable(jsonObject, columnName, rs.getTime(column)) { jsonObject.put(columnName, it.toString()) }
        Types.TIMESTAMP -> putNullable(jsonObject, columnName, rs.getTimestamp(column)) { jsonObject.put(columnName, it.toString()) }
        Types.NULL -> JsonValue.NULL
        Types.BINARY -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.VARBINARY -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.LONGVARBINARY -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.OTHER -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.JAVA_OBJECT -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.DISTINCT -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.STRUCT -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.ARRAY -> {
            val array = rs.getArray(column)
            val jsonArray = MutableJsArray()
            val jsonValues = Rows(array.resultSet).map { toJsonObject.invoke(it) }
            jsonValues.forEach {
                jsonArray.add(it)
            }

            jsonObject.put(columnName, jsonArray)
        }
        Types.BLOB -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.CLOB -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.REF -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.DATALINK -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.BOOLEAN -> {
            val value = rs.getBoolean(column)
            if (rs.wasNull()) JsonValue.NULL
            else if (value) JsonValue.TRUE else JsonValue.FALSE
        }
        Types.ROWID -> {
        }
        Types.NCHAR -> putNullable(jsonObject, columnName, rs.getNString(column)) { jsonObject.put(columnName, it) }
        Types.NVARCHAR -> putNullable(jsonObject, columnName, rs.getNString(column)) { jsonObject.put(columnName, it) }
        Types.LONGNVARCHAR -> putNullable(jsonObject, columnName, rs.getNString(column)) { jsonObject.put(columnName, it) }
        Types.NCLOB -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.SQLXML -> putNullable(jsonObject, columnName, rs.getSQLXML(column)) { jsonObject.put(columnName, it.string) }
        Types.REF_CURSOR -> BoxedJsValue.HAD_INVALID_LITERAL
        Types.TIME_WITH_TIMEZONE -> putNullable(jsonObject, columnName, rs.getTime(column)) { jsonObject.put(columnName, it.toString()) }
        Types.TIMESTAMP_WITH_TIMEZONE -> putNullable(jsonObject, columnName, rs.getTimestamp(column)) { jsonObject.put(columnName, it.toString()) }
        else -> {
            throw IllegalArgumentException("Unknown SQL type: $columnType")
        }
    }
}

val toJsonObject: (Row) -> JsonObject = {
    val metaData = it.rs.metaData
    val jsonObject = MutableJsObject(metaData.columnCount)

    for (column in 1..metaData.columnCount) {
        addJsonValue(jsonObject, column, it.rs)
    }

    jsonObject
}

