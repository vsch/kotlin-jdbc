package com.vladsch.kotlin.jdbc

import kotlin.reflect.KProperty

abstract class ModelPropertyProviderBase<T>(val provider: ModelProperties<T>, val propType: PropertyType) : ModelPropertyProvider<T> {
    final override fun provideDelegate(thisRef: T, prop: KProperty<*>): ModelPropertyProvider<T> {
        return provider.registerProp(prop, propType)
    }

    final override val autoKey: ModelPropertyProvider<T>
        get() = provider.autoKey

    override val key: ModelPropertyProvider<T>
        get() = provider.key

    override val auto: ModelPropertyProvider<T>
        get() = provider.auto

    override val default: ModelPropertyProvider<T>
        get() = provider.default

    final override operator fun <V> getValue(thisRef: T, property: KProperty<*>): V {
        return provider.getValue(thisRef, property)
    }

    final override operator fun <V> setValue(thisRef: T, property: KProperty<*>, value: V) {
        provider.setValue(thisRef, property, value)
    }
}

class ModelPropertyProviderAutoKey<T>(provider: ModelProperties<T>) : ModelPropertyProviderBase<T>(provider, PropertyType.AUTO_KEY) {
    override val key: ModelPropertyProvider<T>
        get() = this

    override val auto: ModelPropertyProvider<T>
        get() = this

    override val default: ModelPropertyProvider<T>
        get() = this
}

class ModelPropertyProviderKey<T>(provider: ModelProperties<T>) : ModelPropertyProviderBase<T>(provider, PropertyType.KEY) {
    override val key: ModelPropertyProvider<T>
        get() = this

    override val auto: ModelPropertyProvider<T>
        get() = provider.autoKey

    override val default: ModelPropertyProvider<T>
        get() = provider.autoKey
}

class ModelPropertyProviderAuto<T>(provider: ModelProperties<T>) : ModelPropertyProviderBase<T>(provider, PropertyType.AUTO) {
    override val key: ModelPropertyProvider<T>
        get() = provider.autoKey

    override val auto: ModelPropertyProvider<T>
        get() = this

    override val default: ModelPropertyProvider<T>
        get() = provider.default
}

class ModelPropertyProviderDefault<T>(provider: ModelProperties<T>) : ModelPropertyProviderBase<T>(provider, PropertyType.DEFAULT) {
    override val key: ModelPropertyProvider<T>
        get() = provider.autoKey

    override val auto: ModelPropertyProvider<T>
        get() = provider.auto

    override val default: ModelPropertyProvider<T>
        get() = this
}

