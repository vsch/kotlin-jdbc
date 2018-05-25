package com.vladsch.kotlin.jdbc

import com.vladsch.boxed.json.BoxedJsValue
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

class ModelProperties<T>(val modelName: String, val allowSetAuto: Boolean = false) : ModelPropertyProvider<T> {
    private val properties = HashMap<String, Any?>()
    private val kProperties = ArrayList<KProperty<*>>()
    private val propTypes = HashMap<String, PropertyType>()
    private val modified = HashSet<String>()

    internal fun snapshot() {
        modified.clear()
    }

    override operator fun provideDelegate(thisRef: T, prop: KProperty<*>): ModelProperties<T> {
        kProperties.add(prop)
        propTypes[prop.name] = PropertyType.PROPERTY
        return this
    }

    // can be used to set immutable properties behind the scenes
    internal fun <V> setProperty(thisRef: T, property: KProperty<*>, value: V) {
        if (!propTypes.containsKey(property.name)) {
            throw IllegalStateException("Attempt to set undefined $modelName.${property.name} property")
        }

        setValue(thisRef, property, value)
    }

    fun isModified(property: KProperty<*>): Boolean {
        if (!propTypes.containsKey(property.name)) {
            throw IllegalStateException("Attempt to test modified status of undefined $modelName.${property.name} property")
        }

        return modified.contains(property.name)
    }

    fun isModified(): Boolean {
        return !modified.isEmpty()
    }

    val autoKey: ModelPropertyProvider<T> = object : ModelPropertyProvider<T> {
        override fun provideDelegate(thisRef: T, prop: KProperty<*>): ModelPropertyProvider<T> {
            kProperties.add(prop)
            propTypes[prop.name] = PropertyType.AUTO_KEY

            if (!allowSetAuto && prop is KMutableProperty) {
                val propSetter = prop.setter.visibility ?: KVisibility.PRIVATE
                if (propSetter != KVisibility.PRIVATE) {
                    throw IllegalStateException("$modelName.${prop.name} auto key property should have no set or have private set")
                }
            }
            return this@ModelProperties
        }

        override val key: ModelPropertyProvider<T>
            get() = this
        override val auto: ModelPropertyProvider<T>
            get() = this
        override val default: ModelPropertyProvider<T>
            get() = this

        override operator fun <V> getValue(thisRef: T, property: KProperty<*>): V {
            return this@ModelProperties.getValue(thisRef, property)
        }

        override operator fun <V> setValue(thisRef: T, property: KProperty<*>, value: V) {
            this@ModelProperties.setValue(thisRef, property, value)
        }
    }

    override val key: ModelPropertyProvider<T> = object : ModelPropertyProvider<T> {
        override fun provideDelegate(thisRef: T, prop: KProperty<*>): ModelPropertyProvider<T> {
            kProperties.add(prop)
            propTypes[prop.name] = PropertyType.KEY
            return this@ModelProperties
        }

        override val key: ModelPropertyProvider<T>
            get() = this
        override val auto: ModelPropertyProvider<T>
            get() = autoKey
        override val default: ModelPropertyProvider<T>
            get() = autoKey

        override operator fun <V> getValue(thisRef: T, property: KProperty<*>): V {
            return this@ModelProperties.getValue(thisRef, property)
        }

        override operator fun <V> setValue(thisRef: T, property: KProperty<*>, value: V) {
            this@ModelProperties.setValue(thisRef, property, value)
        }
    }

    override val auto: ModelPropertyProvider<T> = object : ModelPropertyProvider<T> {
        override fun provideDelegate(thisRef: T, prop: KProperty<*>): ModelPropertyProvider<T> {
            kProperties.add(prop)
            propTypes[prop.name] = PropertyType.AUTO

            if (!allowSetAuto && prop is KMutableProperty) {
                val propSetter = prop.setter.visibility ?: KVisibility.PRIVATE
                if (propSetter != KVisibility.PRIVATE) {
                    throw IllegalStateException("$modelName.${prop.name} auto property should have no set or have private set")
                }
            }
            return this@ModelProperties
        }

        override val key: ModelPropertyProvider<T>
            get() = autoKey
        override val auto: ModelPropertyProvider<T>
            get() = this
        override val default: ModelPropertyProvider<T>
            get() = this@ModelProperties.default

        override operator fun <V> getValue(thisRef: T, property: KProperty<*>): V {
            return this@ModelProperties.getValue(thisRef, property)
        }

        override operator fun <V> setValue(thisRef: T, property: KProperty<*>, value: V) {
            this@ModelProperties.setValue(thisRef, property, value)
        }
    }

    override val default: ModelPropertyProvider<T> = object : ModelPropertyProvider<T> {
        override fun provideDelegate(thisRef: T, prop: KProperty<*>): ModelPropertyProvider<T> {
            kProperties.add(prop)
            propTypes[prop.name] = PropertyType.DEFAULT
            return this@ModelProperties
        }

        override val key: ModelPropertyProvider<T>
            get() = this@ModelProperties.autoKey
        override val auto: ModelPropertyProvider<T>
            get() = this@ModelProperties.auto
        override val default: ModelPropertyProvider<T>
            get() = this

        override operator fun <V> getValue(thisRef: T, property: KProperty<*>): V {
            return this@ModelProperties.getValue(thisRef, property)
        }

        override operator fun <V> setValue(thisRef: T, property: KProperty<*>, value: V) {
            this@ModelProperties.setValue(thisRef, property, value)
        }
    }

    override operator fun <V> getValue(thisRef: T, property: KProperty<*>): V {
        @Suppress("UNCHECKED_CAST")
        return properties[property.name] as V
    }

    override operator fun <V> setValue(thisRef: T, property: KProperty<*>, value: V) {
        if (properties[property.name] != value) {
            properties[property.name] = value
            val propType = propTypes[property.name] ?: PropertyType.PROPERTY
            if (!propType.isAuto) {
                modified.add(property.name)
            }
        }
    }

    @Suppress("USELESS_CAST")
    private fun loadResult(_rs: Row, filter: ((KProperty<*>) -> Boolean)? = null) {
        // load initial properties from result set
        for (prop in kProperties) {
            if (filter != null && !filter(prop)) continue

            val returnType = prop.returnType.classifier ?: continue
            var value: Any?

            try {
                // casts added to catch wrong function call errors
                value = when (returnType) {
                    String::class -> _rs.stringOrNull(prop.name) as String?
                    Byte::class -> _rs.byteOrNull(prop.name) as Byte?
                    Boolean::class -> _rs.booleanOrNull(prop.name) as Boolean?
                    Int::class -> _rs.intOrNull(prop.name) as Int?
                    Long::class -> _rs.longOrNull(prop.name) as Long?
                    Short::class -> _rs.shortOrNull(prop.name) as Short?
                    Double::class -> _rs.doubleOrNull(prop.name) as Double?
                    Float::class -> _rs.floatOrNull(prop.name) as Float?
                    ZonedDateTime::class -> _rs.zonedDateTimeOrNull(prop.name) as ZonedDateTime?
                    OffsetDateTime::class -> _rs.offsetDateTimeOrNull(prop.name) as OffsetDateTime?
                    Instant::class -> _rs.instantOrNull(prop.name) as Instant?
                    LocalDateTime::class -> _rs.localDateTimeOrNull(prop.name) as LocalDateTime?
                    LocalDate::class -> _rs.localDateOrNull(prop.name) as LocalDate?
                    LocalTime::class -> _rs.localTimeOrNull(prop.name) as LocalTime?
                    org.joda.time.DateTime::class -> _rs.jodaDateTimeOrNull(prop.name) as org.joda.time.DateTime?
                    org.joda.time.LocalDateTime::class -> _rs.jodaLocalDateTimeOrNull(prop.name) as org.joda.time.LocalDateTime?
                    org.joda.time.LocalDate::class -> _rs.jodaLocalDateOrNull(prop.name) as org.joda.time.LocalDate?
                    org.joda.time.LocalTime::class -> _rs.jodaLocalTimeOrNull(prop.name) as org.joda.time.LocalTime?
                    java.util.Date::class -> _rs.sqlDateOrNull(prop.name) as java.util.Date?
                    java.sql.Timestamp::class -> _rs.sqlTimestampOrNull(prop.name) as java.sql.Timestamp?
                    java.sql.Time::class -> _rs.sqlTimeOrNull(prop.name) as java.sql.Time?
                    java.sql.Date::class -> _rs.sqlDateOrNull(prop.name) as java.sql.Date?
                    java.sql.SQLXML::class -> _rs.rs.getSQLXML(prop.name) as java.sql.SQLXML?
                    ByteArray::class -> _rs.bytesOrNull(prop.name) as ByteArray?
                    InputStream::class -> _rs.binaryStreamOrNull(prop.name) as InputStream?
                    BigDecimal::class -> _rs.bigDecimalOrNull(prop.name) as BigDecimal?
                    java.sql.Array::class -> _rs.sqlArrayOrNull(prop.name) as java.sql.Array?
                    URL::class -> _rs.urlOrNull(prop.name) as URL?
                    else -> {
                        val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
                        throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${_rs.rs.getObject(prop.name)}, type $className")
                    }
                }
            } catch (e: SQLException) {
                if (!prop.returnType.isMarkedNullable) {
                    throw IllegalArgumentException("$modelName.${prop.name}, is not nullable and result set has no value for ${prop.name}")
                }
                value = null
            }

            if (prop.returnType.isMarkedNullable || value != null) {
                // missing will not be set
                properties.put(prop.name, value)
            } else {
                throw IllegalArgumentException("$modelName.${prop.name}, is not nullable but result set has null value")
            }
        }
    }

    fun load(_rs: Row) {
        loadResult(_rs)
        // have the model loaded, take snapshot
        snapshot()
    }

    fun loadKeys(_rs: Row) {
        loadResult(_rs) {
            propTypes[it.name]?.isKey ?: false
        }

        // have the model loaded, take snapshot
        snapshot()
    }

    // TODO: convert this to using com.fasterxml.jackson if possible
    private fun integralValue(prop: KProperty<*>, json: BoxedJsValue, min: Long, max: Long): Long? {
        if (json.isValid && json.isNumber && json.asJsNumber().isIntegral) {
            val value = json.asJsNumber().longValue()
            if (value < min || value > max) {
                val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
                throw IllegalArgumentException("$modelName.${prop.name} of $value is out of legal range [$min, $max] for $className")
            }
            return value
        } else if (json.hadMissing() || json.hadNull()) {
            return null
        }

        val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
        throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${json.toString()}, type $className")
    }

    private fun doubleValue(prop: KProperty<*>, json: BoxedJsValue, min: Double, max: Double): Double? {
        if (json.isValid && json.isNumber) {
            val value = json.asJsNumber().doubleValue()
            if (value < min || value > max) {
                val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
                throw IllegalArgumentException("$modelName.${prop.name} of $value is out of legal range [$min, $max] for $className")
            }
            return value
        } else if (json.hadMissing() || json.hadNull()) {
            return null
        }

        val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
        throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${json.toString()}, type $className")
    }

    private fun bigDecimalValue(prop: KProperty<*>, json: BoxedJsValue): BigDecimal? {
        if (json.isValid && json.isNumber) {
            return json.asJsNumber().bigDecimalValue()
        } else if (json.hadMissing() || json.hadNull()) {
            return null
        }

        val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
        throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${json.toString()}, type $className")
    }

    private fun stringValue(prop: KProperty<*>, json: BoxedJsValue): String? {
        if (json.isValid && json.isLiteral) {
            if (json.isString) {
                return json.asJsString().string
            } else {
                // just use json toString, it has no quotes
                return json.toString()
            }
        } else if (json.hadMissing() || json.hadNull()) {
            return null
        }

        val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
        throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${json.toString()}, type $className")
    }

    private fun booleanValue(prop: KProperty<*>, json: BoxedJsValue): Boolean? {
        if (json.isValid && (json.isTrue || json.isFalse)) {
            return json.isTrue
        } else if (json.hadMissing() || json.hadNull()) {
            return null
        }

        val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
        throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${json.toString()}, type $className")
    }

    private fun booleanLikeValue(prop: KProperty<*>, json: BoxedJsValue): Boolean? {
        if (json.isValid) {
            if (json.isTrue || json.isFalse) {
                return json.isTrue
            } else if (json.isNumber) {
                val jsNumber = json.asJsNumber()
                if (jsNumber.isIntegral) {
                    return jsNumber.longValue() != 0L
                } else {
                    val doubleValue = jsNumber.doubleValue()
                    return doubleValue.isFinite() && doubleValue != 0.0
                }
            } else if (json.isString) {
                val string = json.asJsString().string
                return string != "" && string != "0"
            }
        } else if (json.hadMissing() || json.hadNull()) {
            return null
        }

        val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
        throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${json.toString()}, type $className")
    }

    fun <T> parsedString(prop: KProperty<*>, json: BoxedJsValue, parser: (String) -> T): T? {
        val value = stringValue(prop, json) ?: return null
        return parser.invoke(value)
    }

    fun urlString(prop: KProperty<*>, json: BoxedJsValue): URL? {
        val value = stringValue(prop, json) ?: return null
        return URL(value)
    }

    @Suppress("USELESS_CAST")
    fun load(json: JsonObject) {
        // load initial properties from result set
        val _rs = BoxedJson.of(json)
        for (prop in kProperties) {
            val returnType = prop.returnType.classifier ?: continue
            @Suppress("IMPLICIT_CAST_TO_ANY")
            val value: Any? = when (returnType) {
                String::class -> stringValue(prop, _rs.get(prop.name)) as String?
                Byte::class -> integralValue(prop, _rs.get(prop.name), Byte.MIN_VALUE.toLong(), Byte.MAX_VALUE.toLong())?.toByte() as Byte?
                Boolean::class -> booleanLikeValue(prop, _rs.get(prop.name)) as Boolean?
                Int::class -> integralValue(prop, _rs.get(prop.name), Int.MIN_VALUE.toLong(), Int.MAX_VALUE.toLong())?.toInt() as Int?
                Long::class -> integralValue(prop, _rs.get(prop.name), Long.MIN_VALUE, Long.MAX_VALUE) as Long?
                Short::class -> integralValue(prop, _rs.get(prop.name), Short.MIN_VALUE.toLong(), Short.MAX_VALUE.toLong())?.toShort() as Short?
                Double::class -> doubleValue(prop, _rs.get(prop.name), Double.MIN_VALUE.toDouble(), Double.MAX_VALUE.toDouble()) as Double?
                Float::class -> doubleValue(prop, _rs.get(prop.name), Float.MIN_VALUE.toDouble(), Float.MAX_VALUE.toDouble())?.toFloat() as Float?
                BigDecimal::class -> bigDecimalValue(prop, _rs.get(prop.name)) as BigDecimal?
                ZonedDateTime::class -> parsedString(prop, _rs.get(prop.name), ZonedDateTime::parse) as ZonedDateTime?
                OffsetDateTime::class -> parsedString(prop, _rs.get(prop.name), OffsetDateTime::parse) as OffsetDateTime?
                Instant::class -> parsedString(prop, _rs.get(prop.name), Instant::parse) as Instant?
                LocalDateTime::class -> parsedString(prop, _rs.get(prop.name), LocalDateTime::parse) as LocalDateTime?
                LocalDate::class -> parsedString(prop, _rs.get(prop.name), LocalDate::parse) as LocalDate?
                LocalTime::class -> parsedString(prop, _rs.get(prop.name), LocalTime::parse) as LocalTime?
                org.joda.time.DateTime::class -> parsedString(prop, _rs.get(prop.name), org.joda.time.DateTime::parse) as org.joda.time.DateTime?
                org.joda.time.LocalDateTime::class -> parsedString(prop, _rs.get(prop.name), org.joda.time.LocalDateTime::parse) as org.joda.time.LocalDateTime?
                org.joda.time.LocalDate::class -> parsedString(prop, _rs.get(prop.name), org.joda.time.LocalDate::parse) as org.joda.time.LocalDate?
                org.joda.time.LocalTime::class -> parsedString(prop, _rs.get(prop.name), org.joda.time.LocalTime::parse) as org.joda.time.LocalTime?
                java.util.Date::class -> parsedString(prop, _rs.get(prop.name), { java.util.Date(it) }) as java.util.Date?
                java.sql.Timestamp::class -> parsedString(prop, _rs.get(prop.name), { java.sql.Timestamp(java.util.Date.parse(it)) }) as java.sql.Timestamp?
                java.sql.Time::class -> parsedString(prop, _rs.get(prop.name), { java.sql.Time(java.util.Date.parse(it)) }) as java.sql.Time?
                java.sql.Date::class -> parsedString(prop, _rs.get(prop.name), { java.sql.Date(java.util.Date.parse(it)) }) as java.sql.Date?
            //java.sql.SQLXML::class -> parsedString(prop, _rs.get(prop.name).asJsNumber(), java.sql.SQLXML::parse)
            //ByteArray::class -> _rs.get(prop.name).asJsArray()
            //java.sql.Array::class -> _rs.get(prop.name).asJsArray()
                URL::class -> urlString(prop, _rs.get(prop.name))
                else -> {
                    val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
                    throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${json.toString()}, type $className")
                }
            }

            if (prop.returnType.isMarkedNullable || value != null) {
                properties.put(prop.name, value)
            } else {
                val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
                throw IllegalArgumentException("$modelName.${prop.name}, cannot be set from json ${json.toString()}, type $className")
            }
        }
    }

    fun load(other: Model<*>) {
        // load initial properties from result set
        for (prop in kProperties) {
            properties.put(prop.name, if (prop.returnType.isMarkedNullable) {
                other.modelProperties.properties[prop.name]
            } else {
                other.modelProperties.properties[prop.name]!!
            })
        }
    }

    fun sqlInsertQuery(tableName: String): SqlQuery {
        val sb = StringBuilder()
        val sbValues = StringBuilder()
        val params = ArrayList<Any?>()
        var sep = ""

        sb.append("INSERT INTO `$tableName` (")

        for (prop in kProperties) {
            val propType = propTypes[prop.name] ?: PropertyType.PROPERTY
            if (!propType.isAuto) {
                if (properties.containsKey(prop.name)) {
                    sb.append(sep).append("`").append(prop.name).append("`")
                    sbValues.append(sep).append("?")
                    sep = ", "
                    params.add(properties[prop.name])
                } else if (!prop.returnType.isMarkedNullable && !propType.isDefault) {
                    throw IllegalStateException("$modelName.${prop.name} property is not nullable or default and not defined in ${this}")
                }
            }
        }

        sb.append(") VALUES (").append(sbValues).append(")")
        return sqlQuery(sb.toString(), params)
    }

    fun sqlDeleteQuery(tableName: String): SqlQuery {
        val sb = StringBuilder()
        val params = ArrayList<Any?>()
        var sep = ""

        sb.append("DELETE FROM `$tableName` WHERE ")

        for (prop in kProperties) {
            val propType = propTypes[prop.name] ?: PropertyType.PROPERTY
            if (propType.isKey) {
                if (properties.containsKey(prop.name)) {
                    sb.append(sep).append("`").append(prop.name).append("` = ?")
                    sep = " AND "
                    params.add(properties[prop.name])
                } else {
                    throw IllegalStateException("$modelName.${prop.name} key property is not defined in ${this}")
                }
            }
        }

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
        var sep = ""

        sb.append("SELECT * FROM `$tableName` WHERE ")

        for (prop in kProperties) {
            val propType = propTypes[prop.name] ?: PropertyType.PROPERTY
            if (propType.isKey) {
                if (properties.containsKey(prop.name)) {
                    sb.append(sep).append("`").append(prop.name).append("` = ?")
                    sep = " AND "
                    params.add(properties[prop.name])
                } else {
                    throw IllegalStateException("$modelName.${prop.name} key property is not defined in ${this}")
                }
            }
        }

        return sqlQuery(sb.toString(), params)
    }

    fun sqlUpdateQuery(tableName: String): SqlQuery {
        val sb = StringBuilder()
        val params = ArrayList<Any?>()
        var sep = ""

        if (!isModified()) {
            throw IllegalStateException("sqlUpdateQuery requested with no modified column values")
        }

        sb.append("UPDATE `$tableName` SET ")

        for (prop in kProperties) {
            val propType = propTypes[prop.name] ?: PropertyType.PROPERTY
            if (!propType.isAuto && modified.contains(prop.name)) {
                if (properties.containsKey(prop.name)) {
                    sb.append(sep).append("`").append(prop.name).append("` = ?")
                    sep = ", "
                    params.add(properties[prop.name])
                }
            }
        }

        sb.append(" WHERE ")
        sep = ""
        for (prop in kProperties) {
            val propType = propTypes[prop.name] ?: PropertyType.PROPERTY
            if (propType.isKey) {
                if (properties.containsKey(prop.name)) {
                    sb.append(sep).append("`").append(prop.name).append("` = ?")
                    sep = " AND "
                    params.add(properties[prop.name])
                } else {
                    throw IllegalStateException("$modelName.${prop.name} key property is not defined in ${this}")
                }
            }
        }

        return sqlQuery(sb.toString(), params)
    }

    fun sqlSelect(tableName: String): String {
        return "SELECT * FROM `$tableName` "
    }

    fun clearAutoKeys() {
        for (prop in kProperties) {
            if (propTypes[prop.name]!!.isAutoKey) {
                properties.remove(prop.name)
            }
        }
    }

    fun forEach(consumer: (prop: KProperty<*>, propertyType: PropertyType, value: Any?) -> Unit) {
        for (prop in kProperties) {
            if (properties.containsKey(prop.name)) {
                consumer.invoke(prop, propTypes[prop.name]!!, properties[prop.name])
            } else {
                consumer.invoke(prop, propTypes[prop.name]!!, Unit)
            }
        }
    }
}
