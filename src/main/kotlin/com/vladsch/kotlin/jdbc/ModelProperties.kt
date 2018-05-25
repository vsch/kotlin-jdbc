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

    private fun loadResult(_rs: Row, filter: ((KProperty<*>) -> Boolean)? = null) {
        // load initial properties from result set
        for (prop in kProperties) {
            if (filter != null && !filter(prop)) continue

            val returnType = prop.returnType.classifier ?: continue
            var value: Any?

            try {
                value = when (returnType) {
                    String::class -> _rs.stringOrNull(prop.name)
                    Byte::class -> _rs.byteOrNull(prop.name)
                    Boolean::class -> _rs.booleanOrNull(prop.name)
                    Int::class -> _rs.intOrNull(prop.name)
                    Long::class -> _rs.longOrNull(prop.name)
                    Short::class -> _rs.shortOrNull(prop.name)
                    Double::class -> _rs.doubleOrNull(prop.name)
                    Float::class -> _rs.floatOrNull(prop.name)
                    ZonedDateTime::class -> _rs.zonedDateTimeOrNull(prop.name)
                    OffsetDateTime::class -> _rs.offsetDateTimeOrNull(prop.name)
                    Instant::class -> _rs.instantOrNull(prop.name)
                    LocalDateTime::class -> _rs.jodaDateTimeOrNull(prop.name)
                    LocalDate::class -> _rs.localDateOrNull(prop.name)
                    LocalTime::class -> _rs.localTimeOrNull(prop.name)
                    org.joda.time.DateTime::class -> _rs.jodaDateTimeOrNull(prop.name)
                    org.joda.time.LocalDateTime::class -> _rs.localDateTimeOrNull(prop.name)
                    org.joda.time.LocalDate::class -> _rs.localTimeOrNull(prop.name)
                    org.joda.time.LocalTime::class -> _rs.localTimeOrNull(prop.name)
                    java.util.Date::class -> _rs.localDateOrNull(prop.name)
                    java.sql.Timestamp::class -> _rs.sqlTimestampOrNull(prop.name)
                    java.sql.Time::class -> _rs.sqlTimeOrNull(prop.name)
                    java.sql.Date::class -> _rs.sqlDateOrNull(prop.name)
                    java.sql.SQLXML::class -> _rs.rs.getSQLXML(prop.name)
                    ByteArray::class -> _rs.bytesOrNull(prop.name)
                    InputStream::class -> _rs.binaryStreamOrNull(prop.name)
                    BigDecimal::class -> _rs.bigDecimalOrNull(prop.name)
                    java.sql.Array::class -> _rs.sqlArrayOrNull(prop.name)
                    URL::class -> _rs.urlOrNull(prop.name)
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
                val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
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

    fun load(json: JsonObject) {
        // load initial properties from result set
        val _rs = BoxedJson.of(json)
        for (prop in kProperties) {
            val returnType = prop.returnType.classifier ?: continue
            @Suppress("IMPLICIT_CAST_TO_ANY")
            val value: Any? = when (returnType) {
                String::class -> stringValue(prop, _rs.get(prop.name))
                Byte::class -> integralValue(prop, _rs.get(prop.name), Byte.MIN_VALUE.toLong(), Byte.MAX_VALUE.toLong())
                Boolean::class -> booleanLikeValue(prop, _rs.get(prop.name))
                Int::class -> integralValue(prop, _rs.get(prop.name), Int.MIN_VALUE.toLong(), Int.MAX_VALUE.toLong())
                Long::class -> integralValue(prop, _rs.get(prop.name), Long.MIN_VALUE, Long.MAX_VALUE)
                Short::class -> integralValue(prop, _rs.get(prop.name), Short.MIN_VALUE.toLong(), Short.MAX_VALUE.toLong())
                Double::class -> doubleValue(prop, _rs.get(prop.name), Double.MIN_VALUE.toDouble(), Double.MAX_VALUE.toDouble())
                Float::class -> doubleValue(prop, _rs.get(prop.name), Float.MIN_VALUE.toDouble(), Float.MAX_VALUE.toDouble())
                BigDecimal::class -> bigDecimalValue(prop, _rs.get(prop.name))
                ZonedDateTime::class -> parsedString(prop, _rs.get(prop.name), ZonedDateTime::parse)
                OffsetDateTime::class -> parsedString(prop, _rs.get(prop.name), OffsetDateTime::parse)
                Instant::class -> parsedString(prop, _rs.get(prop.name), Instant::parse)
                LocalDateTime::class -> parsedString(prop, _rs.get(prop.name), LocalDateTime::parse)
                LocalDate::class -> parsedString(prop, _rs.get(prop.name), LocalDate::parse)
                LocalTime::class -> parsedString(prop, _rs.get(prop.name), LocalTime::parse)
                org.joda.time.DateTime::class -> parsedString(prop, _rs.get(prop.name), org.joda.time.DateTime::parse)
                org.joda.time.LocalDateTime::class -> parsedString(prop, _rs.get(prop.name), org.joda.time.LocalDateTime::parse)
                org.joda.time.LocalDate::class -> parsedString(prop, _rs.get(prop.name), org.joda.time.LocalDate::parse)
                org.joda.time.LocalTime::class -> parsedString(prop, _rs.get(prop.name), org.joda.time.LocalTime::parse)
                java.util.Date::class -> parsedString(prop, _rs.get(prop.name), java.util.Date::parse)
                java.sql.Timestamp::class -> parsedString(prop, _rs.get(prop.name), java.sql.Timestamp::parse)
                java.sql.Time::class -> parsedString(prop, _rs.get(prop.name), java.sql.Time::parse)
                java.sql.Date::class -> parsedString(prop, _rs.get(prop.name), java.sql.Date::parse)
            //                java.sql.SQLXML::class -> parsedString(prop, _rs.get(prop.name).asJsNumber(), java.sql.SQLXML::parse)
            //                ByteArray::class -> _rs.get(prop.name).asJsArray()
            //                java.sql.Array::class -> _rs.get(prop.name).asJsArray()
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
