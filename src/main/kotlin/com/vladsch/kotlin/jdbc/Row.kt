package com.vladsch.kotlin.jdbc

import org.joda.time.DateTime
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Date
import java.time.*
import java.util.*

/**
 * Represents The current row in the result set.
 *
 * Do Not cache, the underlying ResultSet will move to the next row and all row instance on the result set will reflect the new row
 */
open class Row(val rs: ResultSet) {
    val rowIndex: Int get() = rs.row

    /**
     * Fetches nullable value from ResultSet.
     *
     * NOTE: needed only for types for which an SQL NULL returns a non-null value
     * these are boolean, numeric and not clear if needed for blob types or array
     */
    @Suppress("NOTHING_TO_INLINE")
    private inline fun <A : Any> nullable(v: A): A? {
        return if (rs.wasNull()) null else v
    }

    /**
     * Tests value for null, if not null then applies transformation
     */
    private inline fun <A : Any, B : Any> nullable(v: A?, then: (v: A) -> B): B? {
        return if (v == null) null else then(v)
    }

    fun statementOrNull(): Statement? {
        return nullable(rs.statement)
    }

    fun warningsOrNull(): SQLWarning? {
        return rs.warnings
    }

    fun string(columnIndex: Int): String {
        return stringOrNull(columnIndex)!!
    }

    fun stringOrNull(columnIndex: Int): String? {
        // already returns null if column is null
        return rs.getString(columnIndex)
    }

    fun string(columnLabel: String): String {
        return stringOrNull(columnLabel)!!
    }

    fun stringOrNull(columnLabel: String): String? {
        // already returns null if column is null
        return rs.getString(columnLabel)
    }

    fun any(columnIndex: Int): Any {
        return anyOrNull(columnIndex)!!
    }

    fun anyOrNull(columnIndex: Int): Any? {
        // already returns null if column is null
        return rs.getObject(columnIndex)
    }

    fun any(columnLabel: String): Any {
        return anyOrNull(columnLabel)!!
    }

    fun anyOrNull(columnLabel: String): Any? {
        // already returns null if column is null
        return rs.getObject(columnLabel)
    }

    fun long(columnIndex: Int): Long {
        return longOrNull(columnIndex)!!
    }

    fun longOrNull(columnIndex: Int): Long? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getLong(columnIndex))
    }

    fun long(columnLabel: String): Long {
        return longOrNull(columnLabel)!!
    }

    fun longOrNull(columnLabel: String): Long? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getLong(columnLabel))
    }

    fun bytes(columnIndex: Int): ByteArray {
        return bytesOrNull(columnIndex)!!
    }

    fun bytesOrNull(columnIndex: Int): ByteArray? {
        // already returns null if column is null
        return rs.getBytes(columnIndex)
    }

    fun bytes(columnLabel: String): ByteArray {
        return bytesOrNull(columnLabel)!!
    }

    fun bytesOrNull(columnLabel: String): ByteArray? {
        // already returns null if column is null
        return rs.getBytes(columnLabel)
    }

    fun float(columnIndex: Int): Float {
        return floatOrNull(columnIndex)!!
    }

    fun floatOrNull(columnIndex: Int): Float? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getFloat(columnIndex))
    }

    fun float(columnLabel: String): Float {
        return floatOrNull(columnLabel)!!
    }

    fun floatOrNull(columnLabel: String): Float? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getFloat(columnLabel))
    }

    fun short(columnIndex: Int): Short {
        return shortOrNull(columnIndex)!!
    }

    fun shortOrNull(columnIndex: Int): Short? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getShort(columnIndex))
    }

    fun short(columnLabel: String): Short {
        return shortOrNull(columnLabel)!!
    }

    fun shortOrNull(columnLabel: String): Short? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getShort(columnLabel))
    }

    fun double(columnIndex: Int): Double {
        return doubleOrNull(columnIndex)!!
    }

    fun doubleOrNull(columnIndex: Int): Double? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getDouble(columnIndex))
    }

    fun double(columnLabel: String): Double {
        return doubleOrNull(columnLabel)!!
    }

    fun doubleOrNull(columnLabel: String): Double? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getDouble(columnLabel))
    }

    fun int(columnIndex: Int): Int {
        return intOrNull(columnIndex)!!
    }

    fun intOrNull(columnIndex: Int): Int? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getInt(columnIndex))
    }

    fun int(columnLabel: String): Int {
        return intOrNull(columnLabel)!!
    }

    fun intOrNull(columnLabel: String): Int? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getInt(columnLabel))
    }

    fun jodaDateTime(columnIndex: Int): DateTime {
        return jodaDateTimeOrNull(columnIndex)!!
    }

    fun jodaDateTimeOrNull(columnIndex: Int): DateTime? {
        return nullable(sqlTimestampOrNull(columnIndex)) { DateTime(it) }
    }

    fun jodaDateTime(columnLabel: String): DateTime {
        return jodaDateTimeOrNull(columnLabel)!!
    }

    fun jodaDateTimeOrNull(columnLabel: String): DateTime? {
        return nullable(sqlTimestampOrNull(columnLabel)) { DateTime(it) }
    }

    fun jodaLocalDate(columnIndex: Int): org.joda.time.LocalDate {
        return jodaLocalDateOrNull(columnIndex)!!
    }

    fun jodaLocalDateOrNull(columnIndex: Int): org.joda.time.LocalDate? {
        return nullable(sqlTimestampOrNull(columnIndex)) { DateTime(it).toLocalDate() }
    }

    fun jodaLocalDate(columnLabel: String): org.joda.time.LocalDate {
        return jodaLocalDateOrNull(columnLabel)!!
    }

    fun jodaLocalDateOrNull(columnLabel: String): org.joda.time.LocalDate? {
        return nullable(sqlTimestampOrNull(columnLabel)) { DateTime(it).toLocalDate() }
    }

    fun jodaLocalTime(columnIndex: Int): org.joda.time.LocalTime {
        return jodaLocalTimeOrNull(columnIndex)!!
    }

    fun jodaLocalTimeOrNull(columnIndex: Int): org.joda.time.LocalTime? {
        return nullable(sqlTimestampOrNull(columnIndex)) { DateTime(it).toLocalTime() }
    }

    fun jodaLocalTime(columnLabel: String): org.joda.time.LocalTime {
        return jodaLocalTimeOrNull(columnLabel)!!
    }

    fun jodaLocalTimeOrNull(columnLabel: String): org.joda.time.LocalTime? {
        return nullable(sqlTimestampOrNull(columnLabel)) { DateTime(it).toLocalTime() }
    }

    fun zonedDateTime(columnIndex: Int): ZonedDateTime {
        return zonedDateTimeOrNull(columnIndex)!!
    }

    fun zonedDateTimeOrNull(columnIndex: Int): ZonedDateTime? {
        return nullable(sqlTimestampOrNull(columnIndex)) { ZonedDateTime.ofInstant(it.toInstant(), ZoneId.systemDefault()) }
    }

    fun zonedDateTime(columnLabel: String): ZonedDateTime {
        return zonedDateTimeOrNull(columnLabel)!!
    }

    fun zonedDateTimeOrNull(columnLabel: String): ZonedDateTime? {
        return nullable(sqlTimestampOrNull(columnLabel)) { ZonedDateTime.ofInstant(it.toInstant(), ZoneId.systemDefault()) }
    }

    fun offsetDateTime(columnIndex: Int): OffsetDateTime {
        return offsetDateTimeOrNull(columnIndex)!!
    }

    fun offsetDateTimeOrNull(columnIndex: Int): OffsetDateTime? {
        return nullable(sqlTimestampOrNull(columnIndex)) { OffsetDateTime.ofInstant(it.toInstant(), ZoneId.systemDefault()) }
    }

    fun offsetDateTime(columnLabel: String): OffsetDateTime {
        return offsetDateTimeOrNull(columnLabel)!!
    }

    fun offsetDateTimeOrNull(columnLabel: String): OffsetDateTime? {
        return nullable(sqlTimestampOrNull(columnLabel)) { OffsetDateTime.ofInstant(it.toInstant(), ZoneId.systemDefault()) }
    }

    fun instant(columnIndex: Int): Instant {
        return instantOrNull(columnIndex)!!
    }

    fun instantOrNull(columnIndex: Int): Instant? {
        return sqlTimestampOrNull(columnIndex)?.toInstant()
    }

    fun instant(columnLabel: String): Instant {
        return instantOrNull(columnLabel)!!
    }

    fun instantOrNull(columnLabel: String): Instant? {
        return sqlTimestampOrNull(columnLabel)?.toInstant()
    }

    fun localDateTime(columnIndex: Int): LocalDateTime {
        return localDateTimeOrNull(columnIndex)!!
    }

    fun localDateTimeOrNull(columnIndex: Int): LocalDateTime? {
        return sqlTimestampOrNull(columnIndex)?.toLocalDateTime()
    }

    fun localDateTime(columnLabel: String): LocalDateTime {
        return localDateTimeOrNull(columnLabel)!!
    }

    fun localDateTimeOrNull(columnLabel: String): LocalDateTime? {
        return sqlTimestampOrNull(columnLabel)?.toLocalDateTime()
    }

    fun localDate(columnIndex: Int): LocalDate {
        return localDateOrNull(columnIndex)!!
    }

    fun localDateOrNull(columnIndex: Int): LocalDate? {
        return sqlTimestampOrNull(columnIndex)?.toLocalDateTime()?.toLocalDate()
    }

    fun localDate(columnLabel: String): LocalDate {
        return localDateOrNull(columnLabel)!!
    }

    fun localDateOrNull(columnLabel: String): LocalDate? {
        return sqlTimestampOrNull(columnLabel)?.toLocalDateTime()?.toLocalDate()
    }

    fun localTime(columnIndex: Int): LocalTime {
        return localTimeOrNull(columnIndex)!!
    }

    fun localTimeOrNull(columnIndex: Int): LocalTime? {
        return sqlTimestampOrNull(columnIndex)?.toLocalDateTime()?.toLocalTime()
    }

    fun localTime(columnLabel: String): LocalTime {
        return localTimeOrNull(columnLabel)!!
    }

    fun localTimeOrNull(columnLabel: String): LocalTime? {
        return sqlTimestampOrNull(columnLabel)?.toLocalDateTime()?.toLocalTime()
    }

    fun sqlDate(columnIndex: Int): java.sql.Date {
        return sqlDateOrNull(columnIndex)!!
    }

    fun sqlDateOrNull(columnIndex: Int): java.sql.Date? {
        // already returns null if column is null
        return rs.getDate(columnIndex)
    }

    fun sqlDate(columnLabel: String): java.sql.Date {
        return sqlDateOrNull(columnLabel)!!
    }

    fun sqlDateOrNull(columnLabel: String): java.sql.Date? {
        // already returns null if column is null
        return rs.getDate(columnLabel)
    }

    fun sqlDate(columnIndex: Int, cal: Calendar): Date {
        return sqlDateOrNull(columnIndex, cal)!!
    }

    fun sqlDateOrNull(columnIndex: Int, cal: Calendar): Date? {
        // already returns null if column is null
        return rs.getDate(columnIndex, cal)
    }

    fun sqlDate(columnLabel: String, cal: Calendar): Date {
        return sqlDateOrNull(columnLabel, cal)!!
    }

    fun sqlDateOrNull(columnLabel: String, cal: Calendar): Date? {
        // already returns null if column is null
        return rs.getDate(columnLabel, cal)
    }

    fun boolean(columnIndex: Int): Boolean {
        return rs.getBoolean(columnIndex)
    }

    fun booleanOrNull(columnIndex: Int): Boolean? {
        return nullable(rs.getBoolean(columnIndex))
    }

    fun boolean(columnLabel: String): Boolean {
        return rs.getBoolean(columnLabel)
    }

    fun booleanOrNull(columnLabel: String): Boolean? {
        return nullable(rs.getBoolean(columnLabel))
    }

    fun bigDecimal(columnIndex: Int): BigDecimal {
        return bigDecimalOrNull(columnIndex)!!
    }

    fun bigDecimalOrNull(columnIndex: Int): BigDecimal? {
        // already returns null if column is null
        return rs.getBigDecimal(columnIndex)
    }

    fun bigDecimal(columnLabel: String): BigDecimal {
        return bigDecimalOrNull(columnLabel)!!
    }

    fun bigDecimalOrNull(columnLabel: String): BigDecimal? {
        // already returns null if column is null
        return rs.getBigDecimal(columnLabel)
    }

    fun sqlTime(columnIndex: Int): java.sql.Time {
        return sqlTimeOrNull(columnIndex)!!
    }

    fun sqlTimeOrNull(columnIndex: Int): java.sql.Time? {
        // already returns null if column is null
        return rs.getTime(columnIndex)
    }

    fun sqlTime(columnLabel: String): java.sql.Time {
        return sqlTimeOrNull(columnLabel)!!
    }

    fun sqlTimeOrNull(columnLabel: String): java.sql.Time? {
        // already returns null if column is null
        return rs.getTime(columnLabel)
    }

    fun sqlTime(columnIndex: Int, cal: Calendar): java.sql.Time {
        return sqlTimeOrNull(columnIndex, cal)!!
    }

    fun sqlTimeOrNull(columnIndex: Int, cal: Calendar): java.sql.Time? {
        // already returns null if column is null
        return rs.getTime(columnIndex, cal)
    }

    fun sqlTime(columnLabel: String, cal: Calendar): java.sql.Time {
        return sqlTimeOrNull(columnLabel, cal)!!
    }

    fun sqlTimeOrNull(columnLabel: String, cal: Calendar): java.sql.Time? {
        // already returns null if column is null
        return rs.getTime(columnLabel, cal)
    }

    fun url(columnIndex: Int): URL {
        return urlOrNull(columnIndex)!!
    }

    fun urlOrNull(columnIndex: Int): URL? {
        // already returns null if column is null
        return rs.getURL(columnIndex)
    }

    fun url(columnLabel: String): URL {
        return urlOrNull(columnLabel)!!
    }

    fun urlOrNull(columnLabel: String): URL? {
        // already returns null if column is null
        return rs.getURL(columnLabel)
    }

    fun blob(columnIndex: Int): Blob {
        return blobOrNull(columnIndex)!!
    }

    fun blobOrNull(columnIndex: Int): Blob? {
        // not clear if already returns null if column is null
        return nullable(rs.getBlob(columnIndex))
    }

    fun blob(columnLabel: String): Blob {
        return blobOrNull(columnLabel)!!
    }

    fun blobOrNull(columnLabel: String): Blob? {
        // not clear if already returns null if column is null
        return nullable(rs.getBlob(columnLabel))
    }

    fun byte(columnIndex: Int): Byte {
        return byteOrNull(columnIndex)!!
    }

    fun byteOrNull(columnIndex: Int): Byte? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getByte(columnIndex))
    }

    fun byte(columnLabel: String): Byte {
        return byteOrNull(columnLabel)!!
    }

    fun byteOrNull(columnLabel: String): Byte? {
        // need nullable check, returns 0 if column is null
        return nullable(rs.getByte(columnLabel))
    }

    fun clob(columnIndex: Int): java.sql.Clob {
        return clobOrNull(columnIndex)!!
    }

    fun clobOrNull(columnIndex: Int): java.sql.Clob? {
        // not clear if already returns null if column is null
        return nullable(rs.getClob(columnIndex))
    }

    fun clob(columnLabel: String): java.sql.Clob {
        return clobOrNull(columnLabel)!!
    }

    fun clobOrNull(columnLabel: String): java.sql.Clob? {
        // not clear if already returns null if column is null
        return nullable(rs.getClob(columnLabel))
    }

    fun nClob(columnIndex: Int): java.sql.NClob {
        return nClobOrNull(columnIndex)!!
    }

    fun nClobOrNull(columnIndex: Int): NClob? {
        // not clear if already returns null if column is null
        return nullable(rs.getNClob(columnIndex))
    }

    fun nClob(columnLabel: String): java.sql.NClob {
        return nClobOrNull(columnLabel)!!
    }

    fun nClobOrNull(columnLabel: String): NClob? {
        // not clear if already returns null if column is null
        return nullable(rs.getNClob(columnLabel))
    }

    fun sqlArray(columnIndex: Int): java.sql.Array {
        return sqlArrayOrNull(columnIndex)!!
    }

    fun sqlArrayOrNull(columnIndex: Int): java.sql.Array? {
        // not clear if already returns null if column is null
        return nullable(rs.getArray(columnIndex))
    }

    fun sqlArray(columnLabel: String): java.sql.Array {
        return sqlArrayOrNull(columnLabel)!!
    }

    fun sqlArrayOrNull(columnLabel: String): java.sql.Array? {
        // not clear if already returns null if column is null
        return nullable(rs.getArray(columnLabel))
    }

    fun asciiStream(columnIndex: Int): InputStream {
        return asciiStreamOrNull(columnIndex)!!
    }

    fun asciiStreamOrNull(columnIndex: Int): InputStream? {
        // already returns null if column is null
        return rs.getAsciiStream(columnIndex)
    }

    fun asciiStream(columnLabel: String): InputStream {
        return asciiStreamOrNull(columnLabel)!!
    }

    fun asciiStreamOrNull(columnLabel: String): InputStream? {
        // already returns null if column is null
        return rs.getAsciiStream(columnLabel)
    }

    fun sqlTimestamp(columnIndex: Int): java.sql.Timestamp {
        return sqlTimestampOrNull(columnIndex)!!
    }

    fun sqlTimestampOrNull(columnIndex: Int): java.sql.Timestamp? {
        // already returns null if column is null
        return nullable(rs.getTimestamp(columnIndex))
    }

    fun sqlTimestamp(columnLabel: String): java.sql.Timestamp {
        return sqlTimestampOrNull(columnLabel)!!
    }

    fun sqlTimestampOrNull(columnLabel: String): java.sql.Timestamp? {
        // already returns null if column is null
        return rs.getTimestamp(columnLabel)
    }

    fun sqlTimestamp(columnIndex: Int, cal: Calendar): java.sql.Timestamp {
        return sqlTimestampOrNull(columnIndex, cal)!!
    }

    fun sqlTimestampOrNull(columnIndex: Int, cal: Calendar): java.sql.Timestamp? {
        // already returns null if column is null
        return rs.getTimestamp(columnIndex, cal)
    }

    fun sqlTimestamp(columnLabel: String, cal: Calendar): java.sql.Timestamp {
        return sqlTimestampOrNull(columnLabel, cal)!!
    }

    fun sqlTimestampOrNull(columnLabel: String, cal: Calendar): java.sql.Timestamp? {
        // already returns null if column is null
        return rs.getTimestamp(columnLabel, cal)
    }

    fun sqlSqlXml(columnIndex: Int): java.sql.SQLXML {
        return sqlSqlXmlOrNull(columnIndex)!!
    }

    fun sqlSqlXmlOrNull(columnIndex: Int): java.sql.SQLXML? {
        // not sure if already returns null if column is null
        return nullable(rs.getSQLXML(columnIndex))
    }

    fun sqlSqlXml(columnLabel: String): java.sql.SQLXML {
        return sqlSqlXmlOrNull(columnLabel)!!
    }

    fun sqlSqlXmlOrNull(columnLabel: String): java.sql.SQLXML? {
        // not sure if already returns null if column is null
        return nullable(rs.getSQLXML(columnLabel))
    }

    fun ref(columnIndex: Int): Ref {
        return refOrNull(columnIndex)!!
    }

    fun refOrNull(columnIndex: Int): Ref? {
        // never returns null if column is null since reference is to the underlying SQL type
        return nullable(rs.getRef(columnIndex))
    }

    fun ref(columnLabel: String): Ref {
        return refOrNull(columnLabel)!!
    }

    fun refOrNull(columnLabel: String): Ref? {
        // never returns null if column is null since reference is to the underlying SQL type
        return nullable(rs.getRef(columnLabel))
    }

    fun nCharacterStream(columnIndex: Int): Reader {
        return nCharacterStreamOrNull(columnIndex)!!
    }

    fun nCharacterStreamOrNull(columnIndex: Int): Reader? {
        // already returns null if column is null
        return rs.getNCharacterStream(columnIndex)
    }

    fun nCharacterStream(columnLabel: String): Reader {
        return nCharacterStreamOrNull(columnLabel)!!
    }

    fun nCharacterStreamOrNull(columnLabel: String): Reader? {
        // already returns null if column is null
        return rs.getNCharacterStream(columnLabel)
    }

    fun metaDataOrNull(): ResultSetMetaData {
        return rs.metaData
    }

    fun binaryStream(columnIndex: Int): InputStream {
        return binaryStreamOrNull(columnIndex)!!
    }

    fun binaryStreamOrNull(columnIndex: Int): InputStream? {
        // already returns null if column is null
        return rs.getBinaryStream(columnIndex)
    }

    fun binaryStream(columnLabel: String): InputStream {
        return binaryStreamOrNull(columnLabel)!!
    }

    fun binaryStreamOrNull(columnLabel: String): InputStream? {
        // already returns null if column is null
        return rs.getBinaryStream(columnLabel)
    }
}
