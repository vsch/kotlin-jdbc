package com.vladsch.kotlin.jdbc

import java.lang.IllegalStateException

@Suppress("MemberVisibilityCanBePrivate", "UNCHECKED_CAST", "PropertyName")
abstract class ModelNoData<M : ModelNoData<M>>(session: Session?, sqlTable: String, dbCase: Boolean, allowSetAuto: Boolean = true, quote: String? = null) :
    Model<M, ModelNoData.DummyData>(session, sqlTable, dbCase, allowSetAuto, quote) {
    data class DummyData(val dummy: Int)

    // create a data of this
    override fun toData(): DummyData = throw IllegalStateException("ModelNoData derived ${this::class.simpleName} for ${_db.tableName} should not try to convert to a non-existent data class")
}
