package com.vladsch.kotlin.jdbc

import java.io.Reader
import java.math.BigDecimal
import java.sql.*
import java.sql.Date
import java.util.*

class SqlCallResults(private val stmt: CallableStatement, val withResults: Boolean, private val indexMap: Map<String, Int>) {

    private fun getParamIndex(paramName: String): Int {
        require(indexMap.containsKey(paramName)) { "Param $paramName is not an out param" }
        return indexMap[paramName]!!
    }

    fun forEach(operator: (rs: ResultSet, index: Int) -> Unit) {
        var rsIndex = 0
        while (true) {
            val rs = stmt.resultSet ?: return
            operator.invoke(rs, rsIndex++)
            if (!stmt.moreResults) break
        }
    }

    fun getString(paramName: String): String {
        return getStringOrNull(paramName) ?: throw NullPointerException()
    }

    fun getBoolean(paramName: String): Boolean {
        return getBooleanOrNull(paramName) ?: throw NullPointerException()
    }

    fun getByte(paramName: String): Byte {
        return getByteOrNull(paramName) ?: throw NullPointerException()
    }

    fun getShort(paramName: String): Short {
        return getShortOrNull(paramName) ?: throw NullPointerException()
    }

    fun getInt(paramName: String): Int {
        return getIntOrNull(paramName) ?: throw NullPointerException()
    }

    fun getLong(paramName: String): Long {
        return getLongOrNull(paramName) ?: throw NullPointerException()
    }

    fun getFloat(paramName: String): Float {
        return getFloatOrNull(paramName) ?: throw NullPointerException()
    }

    fun getDouble(paramName: String): Double {
        return getDoubleOrNull(paramName) ?: throw NullPointerException()
    }

    fun getBytes(paramName: String): ByteArray {
        return getBytesOrNull(paramName) ?: throw NullPointerException()
    }

    fun getDate(paramName: String): Date {
        return getDateOrNull(paramName) ?: throw NullPointerException()
    }

    fun getTime(paramName: String): Time {
        return getTimeOrNull(paramName) ?: throw NullPointerException()
    }

    fun getTimestamp(paramName: String): Timestamp {
        return getTimestampOrNull(paramName) ?: throw NullPointerException()
    }

    fun getObject(paramName: String): Any {
        return getObjectOrNull(paramName) ?: throw NullPointerException()
    }

    fun getBigDecimal(paramName: String): BigDecimal {
        return getBigDecimalOrNull(paramName) ?: throw NullPointerException()
    }

    fun getObject(paramName: String, map: Map<String, Class<*>>): Any {
        return getObjectOrNull(paramName) ?: throw NullPointerException()
    }

    fun getRef(paramName: String): Ref {
        return getRefOrNull(paramName) ?: throw NullPointerException()
    }

    fun getBlob(paramName: String): Blob {
        return getBlobOrNull(paramName) ?: throw NullPointerException()
    }

    fun getClob(paramName: String): Clob {
        return getClobOrNull(paramName) ?: throw NullPointerException()
    }

    fun getArray(paramName: String): java.sql.Array {
        return getArrayOrNull(paramName) ?: throw NullPointerException()
    }

    fun getDate(paramName: String, cal: Calendar): Date {
        return getDateOrNull(paramName) ?: throw NullPointerException()
    }

    fun getTime(paramName: String, cal: Calendar): Time {
        return getTimeOrNull(paramName) ?: throw NullPointerException()
    }

    fun getTimestamp(paramName: String, cal: Calendar): Timestamp {
        return getTimestampOrNull(paramName) ?: throw NullPointerException()
    }

    fun getRowId(paramName: String): RowId {
        return getRowIdOrNull(paramName) ?: throw NullPointerException()
    }

    fun getNClob(paramName: String): NClob {
        return getNClobOrNull(paramName) ?: throw NullPointerException()
    }

    fun getSQLXML(paramName: String): SQLXML {
        return getSQLXMLOrNull(paramName) ?: throw NullPointerException()
    }

    fun getNString(paramName: String): String {
        return getNStringOrNull(paramName) ?: throw NullPointerException()
    }

    fun getNCharacterStream(paramName: String): Reader {
        return getNCharacterStreamOrNull(paramName) ?: throw NullPointerException()
    }

    fun getCharacterStream(paramName: String): Reader {
        return getCharacterStreamOrNull(paramName) ?: throw NullPointerException()
    }

    fun getStringOrNull(paramName: String): String? {
        return stmt.getString(getParamIndex(paramName))
    }

    fun getBooleanOrNull(paramName: String): Boolean? {
        val value = stmt.getBoolean(getParamIndex(paramName))
        return if (stmt.wasNull()) null else value
    }

    fun getByteOrNull(paramName: String): Byte? {
        val value = stmt.getByte(getParamIndex(paramName))
        return if (stmt.wasNull()) null else value
    }

    fun getShortOrNull(paramName: String): Short? {
        val value = stmt.getShort(getParamIndex(paramName))
        return if (stmt.wasNull()) null else value
    }

    fun getIntOrNull(paramName: String): Int? {
        val value = stmt.getInt(getParamIndex(paramName))
        return if (stmt.wasNull()) null else value
    }

    fun getLongOrNull(paramName: String): Long? {
        val value = stmt.getLong(getParamIndex(paramName))
        return if (stmt.wasNull()) null else value
    }

    fun getFloatOrNull(paramName: String): Float? {
        val value = stmt.getFloat(getParamIndex(paramName))
        return if (stmt.wasNull()) null else value
    }

    fun getDoubleOrNull(paramName: String): Double? {
        val value = stmt.getDouble(getParamIndex(paramName))
        return if (stmt.wasNull()) null else value
    }

    fun getBytesOrNull(paramName: String): ByteArray? {
        return stmt.getBytes(getParamIndex(paramName))
    }

    fun getDateOrNull(paramName: String): Date? {
        return stmt.getDate(getParamIndex(paramName))
    }

    fun getTimeOrNull(paramName: String): Time? {
        return stmt.getTime(getParamIndex(paramName))
    }

    fun getTimestampOrNull(paramName: String): Timestamp? {
        return stmt.getTimestamp(getParamIndex(paramName))
    }

    fun getObjectOrNull(paramName: String): Any? {
        return stmt.getObject(getParamIndex(paramName))
    }

    fun getBigDecimalOrNull(paramName: String): BigDecimal? {
        return stmt.getBigDecimal(getParamIndex(paramName))
    }

    fun getObjectOrNull(paramName: String, map: Map<String, Class<*>>): Any? {
        return stmt.getObject(getParamIndex(paramName), map)
    }

    fun getRefOrNull(paramName: String): Ref? {
        return stmt.getRef(getParamIndex(paramName))
    }

    fun getBlobOrNull(paramName: String): Blob? {
        return stmt.getBlob(getParamIndex(paramName))
    }

    fun getClobOrNull(paramName: String): Clob? {
        return stmt.getClob(getParamIndex(paramName))
    }

    fun getArrayOrNull(paramName: String): java.sql.Array? {
        return stmt.getArray(getParamIndex(paramName))
    }

    fun getDateOrNull(paramName: String, cal: Calendar): Date? {
        return stmt.getDate(getParamIndex(paramName), cal)
    }

    fun getTimeOrNull(paramName: String, cal: Calendar): Time? {
        return stmt.getTime(getParamIndex(paramName), cal)
    }

    fun getTimestampOrNull(paramName: String, cal: Calendar): Timestamp? {
        return stmt.getTimestamp(getParamIndex(paramName), cal)
    }

    fun getRowIdOrNull(paramName: String): RowId? {
        return stmt.getRowId(getParamIndex(paramName))
    }

    fun getNClobOrNull(paramName: String): NClob? {
        return stmt.getNClob(getParamIndex(paramName))
    }

    fun getSQLXMLOrNull(paramName: String): SQLXML? {
        return stmt.getSQLXML(getParamIndex(paramName))
    }

    fun getNStringOrNull(paramName: String): String? {
        return stmt.getNString(getParamIndex(paramName))
    }

    fun getNCharacterStreamOrNull(paramName: String): Reader? {
        return stmt.getNCharacterStream(getParamIndex(paramName))
    }

    fun getCharacterStreamOrNull(paramName: String): Reader? {
        return stmt.getCharacterStream(getParamIndex(paramName))
    }
}
