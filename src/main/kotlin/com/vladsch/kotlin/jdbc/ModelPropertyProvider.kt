package com.vladsch.kotlin.jdbc

import kotlin.reflect.KProperty

interface ModelPropertyProvider<T> {
    val autoKey: ModelPropertyProvider<T>
    val key: ModelPropertyProvider<T>
    val auto: ModelPropertyProvider<T>
    val default: ModelPropertyProvider<T>
    fun column(columnName: String?):ModelPropertyProvider<T>
    fun default(value:Any? = null):ModelPropertyProvider<T>

    operator fun provideDelegate(thisRef: T, prop: KProperty<*>): ModelPropertyProvider<T>

    operator fun <V> getValue(thisRef: T, property: KProperty<*>): V
    operator fun <V> setValue(thisRef: T, property: KProperty<*>, value: V)
}
