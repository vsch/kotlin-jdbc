package com.vladsch.kotlin.jdbc

import com.vladsch.boxed.json.BoxedJson
import java.io.InputStream
import java.math.BigDecimal
import java.net.URL
import java.sql.SQLException
import java.time.*
import javax.json.JsonObject
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KProperty
import kotlin.reflect.KVisibility

class ModelProperties<T>(val name: String, val dbCase: Boolean, val allowSetAuto: Boolean = false) : InternalModelPropertyProvider<T> {
    private val properties = HashMap<String, Any?>()
    private val kProperties = ArrayList<KProperty<*>>()
    private val propertyTypes = HashMap<String, PropertyType>()
    private val keyProperties = ArrayList<KProperty<*>>()
    private val modified = HashSet<String>()
    private val columnNames = HashMap<String, String>()

    internal fun snapshot() {
        modified.clear()
    }

    override val columnName: String?
        get() = null

    internal fun registerProp(prop: KProperty<*>, propType: PropertyType, columnName: String?): ModelProperties<T> {
        kProperties.add(prop)
        propertyTypes[prop.name] = propType

        if (columnName == null) {
            if (!dbCase) {
                // dbCase, convert camelHumps to _
                val snakeCase = prop.name.toSnakeCase()

                if (snakeCase != prop.name) {
                    columnNames[prop.name] = snakeCase
                }
            }
        } else {
            if (columnName != prop.name) {
                columnNames[prop.name] = columnName
            }
        }

        if (propType.isKey) {
            keyProperties.add(prop)
        }

        if (!allowSetAuto && propType.isAuto && prop is KMutableProperty) {
            val propSetter = prop.setter.visibility ?: KVisibility.PRIVATE
            if (propSetter != KVisibility.PRIVATE) {
                throw IllegalStateException("$name.${prop.name} auto property should have no set or have private set")
            }
        }
        return this
    }

    override operator fun provideDelegate(thisRef: T, prop: KProperty<*>): ModelProperties<T> {
        return registerProp(prop, PropertyType.PROPERTY, columnName)
    }

    // can be used to set immutable properties behind the scenes
    internal fun <V> setProperty(thisRef: T, property: KProperty<*>, value: V) {
        if (!propertyTypes.containsKey(property.name)) {
            throw IllegalStateException("Attempt to set undefined $name.${property.name} property")
        }

        setValue(thisRef, property, value)
    }

    fun isModified(property: KProperty<*>): Boolean {
        if (!propertyTypes.containsKey(property.name)) {
            throw IllegalStateException("Attempt to test modified status of undefined $name.${property.name} property")
        }

        return modified.contains(property.name)
    }

    fun isModified(): Boolean {
        return !modified.isEmpty()
    }

    fun columnName(property: KProperty<*>): String {
        if (!propertyTypes.containsKey(property.name)) {
            throw IllegalStateException("Attempt to get column name of undefined $name.${property.name} property")
        }

        return columnNames[property.name] ?: property.name
    }

    override val autoKey = ModelPropertyProviderAutoKey<T>(this, null)
    override val key = ModelPropertyProviderKey<T>(this, null)
    override val auto = ModelPropertyProviderAuto<T>(this, null)
    override val default = ModelPropertyProviderDefault<T>(this, null)

    override fun column(columnName: String?): ModelPropertyProvider<T> {
        return if (columnName == null) {
            this
        } else {
            ModelPropertyProviderBase<T>(this, PropertyType.PROPERTY, columnName)
        }
    }

    override operator fun <V> getValue(thisRef: T, property: KProperty<*>): V {
        @Suppress("UNCHECKED_CAST")
        return properties[property.name] as V
    }

    override operator fun <V> setValue(thisRef: T, property: KProperty<*>, value: V) {
        if (properties[property.name] !== value) {
            properties[property.name] = value
            val propType = propertyTypes[property.name] ?: PropertyType.PROPERTY
            if (!propType.isAuto) {
                modified.add(property.name)
            }
        }
    }

    @Suppress("USELESS_CAST")
    private fun loadResult(row: Row, filter: ((KProperty<*>) -> Boolean)? = null) {
        // load initial properties from result set
        for (prop in kProperties) {
            if (filter != null && !filter(prop)) continue

            val returnType = prop.returnType.classifier ?: continue
            var value: Any?

            try {
                // casts added to catch wrong function call errors
                val columnName = columnNames[prop.name] ?: prop.name

                value = when (returnType) {
                    String::class -> row.stringOrNull(columnName) as String?
                    Byte::class -> row.byteOrNull(columnName) as Byte?
                    Boolean::class -> row.booleanOrNull(columnName) as Boolean?
                    Int::class -> row.intOrNull(columnName) as Int?
                    Long::class -> row.longOrNull(columnName) as Long?
                    Short::class -> row.shortOrNull(columnName) as Short?
                    Double::class -> row.doubleOrNull(columnName) as Double?
                    Float::class -> row.floatOrNull(columnName) as Float?
                    ZonedDateTime::class -> row.zonedDateTimeOrNull(columnName) as ZonedDateTime?
                    OffsetDateTime::class -> row.offsetDateTimeOrNull(columnName) as OffsetDateTime?
                    Instant::class -> row.instantOrNull(columnName) as Instant?
                    LocalDateTime::class -> row.localDateTimeOrNull(columnName) as LocalDateTime?
                    LocalDate::class -> row.localDateOrNull(columnName) as LocalDate?
                    LocalTime::class -> row.localTimeOrNull(columnName) as LocalTime?
                    org.joda.time.DateTime::class -> row.jodaDateTimeOrNull(columnName) as org.joda.time.DateTime?
                    org.joda.time.LocalDateTime::class -> row.jodaLocalDateTimeOrNull(columnName) as org.joda.time.LocalDateTime?
                    org.joda.time.LocalDate::class -> row.jodaLocalDateOrNull(columnName) as org.joda.time.LocalDate?
                    org.joda.time.LocalTime::class -> row.jodaLocalTimeOrNull(columnName) as org.joda.time.LocalTime?
                    java.util.Date::class -> row.sqlDateOrNull(columnName) as java.util.Date?
                    java.sql.Timestamp::class -> row.sqlTimestampOrNull(columnName) as java.sql.Timestamp?
                    java.sql.Time::class -> row.sqlTimeOrNull(columnName) as java.sql.Time?
                    java.sql.Date::class -> row.sqlDateOrNull(columnName) as java.sql.Date?
                    java.sql.SQLXML::class -> row.rs.getSQLXML(columnName) as java.sql.SQLXML?
                    ByteArray::class -> row.bytesOrNull(columnName) as ByteArray?
                    InputStream::class -> row.binaryStreamOrNull(columnName) as InputStream?
                    BigDecimal::class -> row.bigDecimalOrNull(columnName) as BigDecimal?
                    java.sql.Array::class -> row.sqlArrayOrNull(columnName) as java.sql.Array?
                    URL::class -> row.urlOrNull(columnName) as URL?
                    else -> {
                        val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
                        throw IllegalArgumentException("$name.${columnName} cannot be set from json ${row.rs.getObject(columnName)}, type $className")
                    }
                }
            } catch (e: SQLException) {
                if (!prop.returnType.isMarkedNullable) {
                    throw IllegalArgumentException("$name.${prop.name}, is not nullable and result set has no value for ${prop.name}")
                }
                value = null
            }

            if (prop.returnType.isMarkedNullable || value != null) {
                // missing will not be set
                properties.put(prop.name, value)
            } else {
                throw IllegalArgumentException("$name.${prop.name}, is not nullable but result set has null value")
            }
        }
    }

    fun load(row: Row) {
        loadResult(row)
        // have the model loaded, take snapshot
        snapshot()
    }

    fun loadKeys(row: Row) {
        loadResult(row) {
            propertyTypes[it.name]?.isKey ?: false
        }

        // have the model loaded, take snapshot
        snapshot()
    }

    @Suppress("USELESS_CAST")
    fun load(json: JsonObject) {
        // load initial properties from result set
        val boxed = BoxedJson.of(json)
        for (prop in kProperties) {
            val returnType = prop.returnType.classifier ?: continue
            @Suppress("IMPLICIT_CAST_TO_ANY")
            val value: Any? = when (returnType) {
                String::class -> stringValue(name, prop, boxed.get(prop.name)) as String?
                Byte::class -> integralValue(name, prop, boxed.get(prop.name), Byte.MIN_VALUE.toLong(), Byte.MAX_VALUE.toLong())?.toByte() as Byte?
                Boolean::class -> booleanLikeValue(name, prop, boxed.get(prop.name)) as Boolean?
                Int::class -> integralValue(name, prop, boxed.get(prop.name), Int.MIN_VALUE.toLong(), Int.MAX_VALUE.toLong())?.toInt() as Int?
                Long::class -> integralValue(name, prop, boxed.get(prop.name), Long.MIN_VALUE, Long.MAX_VALUE) as Long?
                Short::class -> integralValue(name, prop, boxed.get(prop.name), Short.MIN_VALUE.toLong(), Short.MAX_VALUE.toLong())?.toShort() as Short?
                Double::class -> doubleValue(name, prop, boxed.get(prop.name), Double.MIN_VALUE.toDouble(), Double.MAX_VALUE.toDouble()) as Double?
                Float::class -> doubleValue(name, prop, boxed.get(prop.name), Float.MIN_VALUE.toDouble(), Float.MAX_VALUE.toDouble())?.toFloat() as Float?
                BigDecimal::class -> bigDecimalValue(name, prop, boxed.get(prop.name)) as BigDecimal?
                ZonedDateTime::class -> parsedString(name, prop, boxed.get(prop.name), ZonedDateTime::parse) as ZonedDateTime?
                OffsetDateTime::class -> parsedString(name, prop, boxed.get(prop.name), OffsetDateTime::parse) as OffsetDateTime?
                Instant::class -> parsedString(name, prop, boxed.get(prop.name), Instant::parse) as Instant?
                LocalDateTime::class -> parsedString(name, prop, boxed.get(prop.name), LocalDateTime::parse) as LocalDateTime?
                LocalDate::class -> parsedString(name, prop, boxed.get(prop.name), LocalDate::parse) as LocalDate?
                LocalTime::class -> parsedString(name, prop, boxed.get(prop.name), LocalTime::parse) as LocalTime?
                org.joda.time.DateTime::class -> parsedString(name, prop, boxed.get(prop.name), org.joda.time.DateTime::parse) as org.joda.time.DateTime?
                org.joda.time.LocalDateTime::class -> parsedString(name, prop, boxed.get(prop.name), org.joda.time.LocalDateTime::parse) as org.joda.time.LocalDateTime?
                org.joda.time.LocalDate::class -> parsedString(name, prop, boxed.get(prop.name), org.joda.time.LocalDate::parse) as org.joda.time.LocalDate?
                org.joda.time.LocalTime::class -> parsedString(name, prop, boxed.get(prop.name), org.joda.time.LocalTime::parse) as org.joda.time.LocalTime?
                java.util.Date::class -> parsedString(name, prop, boxed.get(prop.name), { java.util.Date(it) }) as java.util.Date?
                java.sql.Timestamp::class -> parsedString(name, prop, boxed.get(prop.name), { java.sql.Timestamp(java.util.Date.parse(it)) }) as java.sql.Timestamp?
                java.sql.Time::class -> parsedString(name, prop, boxed.get(prop.name), { java.sql.Time(java.util.Date.parse(it)) }) as java.sql.Time?
                java.sql.Date::class -> parsedString(name, prop, boxed.get(prop.name), { java.sql.Date(java.util.Date.parse(it)) }) as java.sql.Date?
            //java.sql.SQLXML::class -> parsedString(prop, _rs.get(prop.name).asJsNumber(), java.sql.SQLXML::parse)
            //ByteArray::class -> _rs.get(prop.name).asJsArray()
            //java.sql.Array::class -> _rs.get(prop.name).asJsArray()
                URL::class -> urlString(name, prop, boxed.get(prop.name))
                else -> {
                    val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
                    throw IllegalArgumentException("$name.${prop.name} cannot be set from json ${json.toString()}, type $className")
                }
            }

            if (prop.returnType.isMarkedNullable || value != null) {
                properties.put(prop.name, value)
            } else {
                val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
                throw IllegalArgumentException("$name.${prop.name}, cannot be set from json ${json.toString()}, type $className")
            }
        }
    }

    fun load(other: Model<*>) {
        // load initial properties from result set
        for (prop in kProperties) {
            properties.put(prop.name, if (prop.returnType.isMarkedNullable) {
                other._model.properties[prop.name]
            } else {
                other._model.properties[prop.name]!!
            })
        }
    }

    @Suppress("ReplacePutWithAssignment")
    fun toJsonObject(): JsonObject {
        val jsonObject = BoxedJson.of()
        for ((name, value) in properties) {
            if (value == null) {
                jsonObject.putNull(name)
            } else {
                when (value) {
                    is String -> jsonObject.put(name, value)
                    is Byte -> jsonObject.put(name, value.toInt())
                    is Boolean -> jsonObject.put(name, value)
                    is Int -> jsonObject.put(name, value)
                    is Long -> jsonObject.put(name, value)
                    is Short -> jsonObject.put(name, value.toInt())
                    is Double -> jsonObject.put(name, value)
                    is Float -> jsonObject.put(name, value)
                    is BigDecimal -> jsonObject.put(name, value)
                    is ZonedDateTime -> jsonObject.put(name, value.toString())
                    is OffsetDateTime -> jsonObject.put(name, value.toString())
                    is Instant -> jsonObject.put(name, value.toString())
                    is LocalDateTime -> jsonObject.put(name, value.toString())
                    is LocalDate -> jsonObject.put(name, value.toString())
                    is LocalTime -> jsonObject.put(name, value.toString())
                    is org.joda.time.DateTime -> jsonObject.put(name, value.toString())
                    is org.joda.time.LocalDateTime -> jsonObject.put(name, value.toString())
                    is org.joda.time.LocalDate -> jsonObject.put(name, value.toString())
                    is org.joda.time.LocalTime -> jsonObject.put(name, value.toString())
                    is java.util.Date -> jsonObject.put(name, value.toString())
                    is java.sql.Timestamp -> jsonObject.put(name, value.toString())
                    is java.sql.Time -> jsonObject.put(name, value.toString())
                    is java.sql.Date -> jsonObject.put(name, value.toString())
                }
            }
        }

        return jsonObject
    }

    fun sqlInsertQuery(tableName: String): SqlQuery {
        val sb = StringBuilder()
        val sbValues = StringBuilder()
        val params = ArrayList<Any?>()
        var sep = ""

        sb.append("INSERT INTO `$tableName` (")

        for (prop in kProperties) {
            val propType = propertyTypes[prop.name] ?: PropertyType.PROPERTY

            if (!propType.isAuto) {
                if (properties.containsKey(prop.name)) {
                    val propValue = properties[prop.name]
                    // skip null properties which have defaults (null means use default)
                    if (propValue != null || !propType.isDefault) {
                        val columnName = columnNames[prop.name] ?: prop.name
                        sb.append(sep).append("`").append(columnName).append("`")
                        sbValues.append(sep).append("?")
                        sep = ", "
                        params.add(propValue)
                    }
                } else if (!prop.returnType.isMarkedNullable && !propType.isDefault) {
                    throw IllegalStateException("$name.${prop.name} property is not nullable nor default and not defined in ${this}")
                }
            }
        }

        sb.append(") VALUES (").append(sbValues).append(")")
        return sqlQuery(sb.toString(), params)
    }

    @Suppress("MemberVisibilityCanBePrivate")
    fun appendKeys(appendable: Appendable, params: ArrayList<Any?>, delimiter: String = " AND ", sep: String = ""): String {
        var useSep = sep

        for (prop in keyProperties) {
            if (properties.containsKey(prop.name)) {
                val columnName = columnNames[prop.name] ?: prop.name

                appendable.append(useSep).append("`").append(columnName).append("` = ?")
                useSep = delimiter
                params.add(properties[prop.name])
            } else {
                throw IllegalStateException("$name.${prop.name} key property is not defined in ${this}")
            }
        }
        return useSep
    }

    fun sqlDeleteQuery(tableName: String): SqlQuery {
        val sb = StringBuilder()
        val params = ArrayList<Any?>()

        sb.append("DELETE FROM `$tableName` WHERE ")
        appendKeys(sb, params)
        return sqlQuery(sb.toString(), params)
    }

    /**
     * Return select for keys of the model
     * @param tableName String
     * @return SqlQuery
     */
    fun sqlSelectQuery(tableName: String): SqlQuery {
        val sb = StringBuilder()
        val params = ArrayList<Any?>()

        sb.append("SELECT * FROM `$tableName` WHERE ")
        appendKeys(sb, params)
        return sqlQuery(sb.toString(), params)
    }

    fun sqlUpdateQuery(tableName: String): SqlQuery {
        val sb = StringBuilder()
        val params = ArrayList<Any?>()
        var sep = ""

        if (!isModified()) {
            throw IllegalStateException("sqlUpdateQuery requested with no modified model properties")
        }

        sb.append("UPDATE `$tableName` SET ")

        for (prop in kProperties) {
            val propType = propertyTypes[prop.name] ?: PropertyType.PROPERTY
            if (!propType.isAuto) {
                if (properties.containsKey(prop.name)) {
                    val propValue = properties[prop.name]
                    if (modified.contains(prop.name)) {
                        // skip null properties which have defaults (null means use default)
                        if (propValue != null || !propType.isDefault) {
                            val columnName = columnNames[prop.name] ?: prop.name

                            sb.append(sep).append("`").append(columnName).append("` = ?")
                            sep = ", "
                            params.add(properties[prop.name])
                        }
                    }
                }
            }
        }

        if (params.isEmpty()) {
            // all modified props were null with defaults, update key to itself
            val prop = keyProperties[0]
            val columnName = columnNames[prop.name] ?: prop.name
            sb.append(sep).append("`").append(columnName).append("` = `").append(columnName).append("`")
        }

        sb.append(" WHERE ")
        appendKeys(sb, params)
        return sqlQuery(sb.toString(), params)
    }

    fun sqlSelectTable(tableName: String): String {
        return "SELECT * FROM `$tableName` "
    }

    fun clearAutoKeys() {
        for (prop in keyProperties) {
            if (propertyTypes[prop.name]!!.isAutoKey) {
                properties.remove(prop.name)
            }
        }
    }

    fun forEachKey(consumer: (prop: KProperty<*>, propType: PropertyType, value: Any?) -> Unit) {
        for (prop in keyProperties) {
            if (properties.containsKey(prop.name)) {
                consumer.invoke(prop, propertyTypes[prop.name]!!, properties[prop.name])
            } else {
                consumer.invoke(prop, propertyTypes[prop.name]!!, Unit)
            }
        }
    }

    fun forEachProp(consumer: (prop: KProperty<*>, propType: PropertyType, value: Any?) -> Unit) {
        for (prop in kProperties) {
            if (properties.containsKey(prop.name)) {
                consumer.invoke(prop, propertyTypes[prop.name]!!, properties[prop.name])
            } else {
                consumer.invoke(prop, propertyTypes[prop.name]!!, Unit)
            }
        }
    }

    fun forEachKey(consumer: (prop: KProperty<*>, propType: PropertyType, columnName: String, value: Any?) -> Unit) {
        for (prop in keyProperties) {
            val columnName = columnNames[prop.name] ?: prop.name
            if (properties.containsKey(prop.name)) {
                consumer.invoke(prop, propertyTypes[prop.name]!!, columnName, properties[prop.name])
            } else {
                consumer.invoke(prop, propertyTypes[prop.name]!!, columnName, Unit)
            }
        }
    }

    fun forEachProp(consumer: (prop: KProperty<*>, propType: PropertyType, columnName: String, value: Any?) -> Unit) {
        for (prop in kProperties) {
            val columnName = columnNames[prop.name] ?: prop.name
            if (properties.containsKey(prop.name)) {
                consumer.invoke(prop, propertyTypes[prop.name]!!, columnName, properties[prop.name])
            } else {
                consumer.invoke(prop, propertyTypes[prop.name]!!, columnName, Unit)
            }
        }
    }
}
