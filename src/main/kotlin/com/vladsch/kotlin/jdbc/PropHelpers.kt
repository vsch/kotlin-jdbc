package com.vladsch.kotlin.jdbc

import com.vladsch.boxed.json.BoxedJsValue
import java.math.BigDecimal
import java.net.URL
import kotlin.reflect.KProperty

// TODO: convert this to using com.fasterxml.jackson if possible
fun integralValue(modelName: String, prop: KProperty<*>, json: BoxedJsValue, min: Long, max: Long): Long? {
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

fun doubleValue(modelName: String, prop: KProperty<*>, json: BoxedJsValue, min: Double, max: Double): Double? {
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

fun bigDecimalValue(modelName: String, prop: KProperty<*>, json: BoxedJsValue): BigDecimal? {
    if (json.isValid && json.isNumber) {
        return json.asJsNumber().bigDecimalValue()
    } else if (json.hadMissing() || json.hadNull()) {
        return null
    }

    val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
    throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${json.toString()}, type $className")
}

fun stringValue(modelName: String, prop: KProperty<*>, json: BoxedJsValue): String? {
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

fun booleanValue(modelName: String, prop: KProperty<*>, json: BoxedJsValue): Boolean? {
    if (json.isValid && (json.isTrue || json.isFalse)) {
        return json.isTrue
    } else if (json.hadMissing() || json.hadNull()) {
        return null
    }

    val className = (prop.returnType.classifier as? Class<*>)?.simpleName ?: "<unknown>"
    throw IllegalArgumentException("$modelName.${prop.name} cannot be set from json ${json.toString()}, type $className")
}

fun booleanLikeValue(modelName: String, prop: KProperty<*>, json: BoxedJsValue): Boolean? {
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

fun <T> parsedString(modelName: String, prop: KProperty<*>, json: BoxedJsValue, parser: (String) -> T): T? {
    val value = stringValue(modelName, prop, json) ?: return null
    return parser.invoke(value)
}

fun urlString(modelName: String, prop: KProperty<*>, json: BoxedJsValue): URL? {
    val value = stringValue(modelName, prop, json) ?: return null
    return URL(value)
}

