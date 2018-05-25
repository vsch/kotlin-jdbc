package com.vladsch.kotlin.jdbc

enum class PropertyType(val isKey: Boolean, val isAuto: Boolean, val isDefault: Boolean) {
    PROPERTY(false, false, false),
    KEY(true, false, false),
    AUTO(false, true, false),
    AUTO_KEY(true, true, false),
    DEFAULT(false, false, true),
    ;

    val isAutoKey get() = isAuto && isKey
}
