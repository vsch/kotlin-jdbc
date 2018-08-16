package com.vladsch.kotlin.jdbc

import javax.json.JsonObject

abstract class ModelCompanion<M:Model<M>, D> {
    abstract val tableName:String
    abstract fun createModel(): M
    abstract fun createData(model:M): D

    val toModel: (Row) -> M = { row ->
        createModel().load(row)
    }

    val toData: (Row) -> D = { row ->
        createData(createModel().load(row))
    }

    val toJson: (Row) -> JsonObject = { row ->
        createModel().load(row).toJson()
    }

    fun listQuery(vararg params: Pair<String, Any?>): SqlQuery {
        return Model.listQuery(tableName, params)
    }

    fun listQuery(params: Map<String, Any?>): SqlQuery {
        return Model.listQuery(tableName, params)
    }
}
