package com.vladsch.kotlin.jdbc

import com.vladsch.boxed.json.BoxedJsValue
import com.vladsch.boxed.json.MutableJsArray
import com.vladsch.boxed.json.MutableJsObject
import org.joda.time.LocalDateTime
import java.io.*
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

fun sqlQuery(statement: String, params: List<Any?>): SqlQuery {
    return SqlQuery(statement, params = params)
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

fun session(url: String, user: String, password: String): Session {
    val conn = DriverManager.getConnection(url, user, password)
    return Session(Connection(conn))
}

fun session(dataSource: DataSource): Session {
    return Session(Connection(dataSource.connection))
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

fun getResourceFiles(resourceClass: Class<*>, path: String): List<String> {
    val filenames = ArrayList<String>()

    getResourceAsStream(resourceClass, path)?.use { inputStream ->
        BufferedReader(InputStreamReader(inputStream)).use { br ->
            while (true) {
                val resource = br.readLine() ?: break
                filenames.add(resource)
            }
        }
    }

    return filenames
}

fun StringBuilder.streamAppend(inputStream: InputStream) {
    BufferedReader(InputStreamReader(inputStream)).use { br ->
        while (true) {
            val resource = br.readLine() ?: break
            this.append(resource).append('\n')
        }
    }
}

fun getResourceAsString(resourceClass: Class<*>, path: String): String {
    val sb = StringBuilder()

    getResourceAsStream(resourceClass, path)?.use { inputStream ->
        sb.streamAppend(inputStream)
    }

    return sb.toString()
}

fun getFileContent(file: File): String {
    val inputStream = FileInputStream(file)
    val sb = StringBuilder()
    sb.streamAppend(inputStream)
    return sb.toString()
}

fun getResourceAsStream(resourceClass: Class<*>, resource: String): InputStream? {
    return resourceClass.getResourceAsStream(resource)
}

operator fun File.plus(name: String): File {
    val path = this.path
    val dbDir = File(if (!path.endsWith('/') && !name.startsWith('/')) "$path/$name" else "$path$name")
    return dbDir
}

fun File.ensureExistingDirectory(paramName: String = "directory"): File {
    if (!this.exists() || !this.isDirectory) {
        throw IllegalArgumentException("$paramName '${this.path}' must point to existing directory")
    }
    return this
}

fun File.ensureCreateDirectory(paramName: String = "directory"): File {
    if (!this.exists()) {
        if (!this.mkdir()) {
            throw IllegalStateException("could not create directory $paramName '${this.path}' must point to existing directory")
        }
    }
    if (!this.isDirectory) {
        throw IllegalStateException("$paramName '${this.path}' exists and is not a directory")
    }
    return this
}

fun String.versionCompare(other: String): Int {
    val theseParts = this.removePrefix("V").split('_', limit = 4)
    val otherParts = other.removePrefix("V").split('_', limit = 4)

    val iMax = Math.min(theseParts.size, otherParts.size)
    for (i in 0 until iMax) {
        if (i < 3) {
            // use integer compare
            val thisVersion = theseParts[i].toInt()
            val otherVersion = otherParts[i].toInt()
            if (thisVersion != otherVersion) {
                return thisVersion.compareTo(otherVersion)
            }
        } else {
            return theseParts[i].compareTo(otherParts[i])
        }
    }
    return 0
}

fun getVersionDirectory(dbDir: File, dbVersion: String, createDir: Boolean?): File {
    if (createDir != null) {
        dbDir.ensureExistingDirectory("dbDir")
    }

    val dbVersionDir = dbDir + dbVersion

    if (createDir == true) {
        dbVersionDir.ensureCreateDirectory("dbDir/dbVersion")
    } else if (createDir == false) {
        dbVersionDir.ensureExistingDirectory("dbDir/dbVersion")
    }
    return dbVersionDir
}

