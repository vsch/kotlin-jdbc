package com.vladsch.kotlin.jdbc

import kotlin.reflect.KProperty

open class ModelPropertyProviderBase<T>(val provider: ModelProperties<T>, val propType: PropertyType, override val columnName: String?, override val defaultValue:Any? = Unit) : InternalModelPropertyProvider<T> {
    final override fun provideDelegate(thisRef: T, prop: KProperty<*>): ModelProperties<T> {
        return provider.registerProp(prop, propType, columnName, defaultValue)
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

    override fun column(columnName: String?): ModelPropertyProvider<T> {
        return if (this.columnName == columnName) {
            this
        } else {
            if (columnName == null) {
                provider
            } else {
                ModelPropertyProviderBase<T>(provider, propType, columnName)
            }
        }
    }

    override fun default(value: Any?): ModelPropertyProvider<T> {
        return ModelPropertyProviderBase<T>(provider, propType, columnName, value)
    }
}

class ModelPropertyProviderAutoKey<T>(provider: ModelProperties<T>, columnName: String?) : ModelPropertyProviderBase<T>(provider, PropertyType.AUTO_KEY, columnName) {
    override val key: ModelPropertyProvider<T>
        get() = this

    override val auto: ModelPropertyProvider<T>
        get() = this

    override val default: ModelPropertyProvider<T>
        get() = this

    override fun column(columnName: String?): ModelPropertyProvider<T> {
        return if (this.columnName == columnName) {
            this
        } else {
            if (columnName == null) {
                provider.autoKey
            } else {
                ModelPropertyProviderAutoKey<T>(provider, columnName)
            }
        }
    }

    override fun default(value: Any?): ModelPropertyProvider<T> {
        if (value === Unit) return this
        throw IllegalStateException("Auto Key column cannot have a default value")
    }
}

class ModelPropertyProviderKey<T>(provider: ModelProperties<T>, columnName: String?) : ModelPropertyProviderBase<T>(provider, PropertyType.KEY, columnName) {
    override val key: ModelPropertyProvider<T>
        get() = this

    override val auto: ModelPropertyProvider<T>
        get() = provider.autoKey

    override val default: ModelPropertyProvider<T>
        get() = provider.autoKey

    override fun column(columnName: String?): ModelPropertyProvider<T> {
        return if (this.columnName == columnName) {
            this
        } else {
            if (columnName == null) {
                provider.autoKey
            } else {
                ModelPropertyProviderKey<T>(provider, columnName)
            }
        }
    }

    override fun default(value: Any?): ModelPropertyProvider<T> {
        if (value === Unit) return this
        throw IllegalStateException("Key column cannot have a default value")
    }
}

class ModelPropertyProviderAuto<T>(provider: ModelProperties<T>, columnName: String?) : ModelPropertyProviderBase<T>(provider, PropertyType.AUTO, columnName) {
    override val key: ModelPropertyProvider<T>
        get() = provider.autoKey

    override val auto: ModelPropertyProvider<T>
        get() = this

    override val default: ModelPropertyProvider<T>
        get() = provider.default

    override fun column(columnName: String?): ModelPropertyProvider<T> {
        return if (this.columnName == columnName) {
            this
        } else {
            if (columnName == null) {
                provider.autoKey
            } else {
                ModelPropertyProviderAuto<T>(provider, columnName)
            }
        }
    }

    override fun default(value: Any?): ModelPropertyProvider<T> {
        if (value === Unit) return this
        throw IllegalStateException("Auto column cannot have a default value")
    }
}

class ModelPropertyProviderDefault<T>(provider: ModelProperties<T>, columnName: String?, value: Any?) : ModelPropertyProviderBase<T>(provider, PropertyType.DEFAULT, columnName, value) {
    override val key: ModelPropertyProvider<T>
        get() = provider.autoKey

    override val auto: ModelPropertyProvider<T>
        get() = provider.auto

    override val default: ModelPropertyProvider<T>
        get() = this

    override fun column(columnName: String?): ModelPropertyProvider<T> {
        return if (this.columnName == columnName) {
            this
        } else {
            if (columnName == null) {
                provider.autoKey
            } else {
                ModelPropertyProviderDefault<T>(provider, columnName, defaultValue)
            }
        }
    }

    override fun default(value: Any?): ModelPropertyProvider<T> {
        if (value === defaultValue) return this
        return ModelPropertyProviderBase<T>(provider, propType, columnName, value)
    }
}

