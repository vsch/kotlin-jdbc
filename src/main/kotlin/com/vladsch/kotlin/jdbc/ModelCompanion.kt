package com.vladsch.kotlin.jdbc

import javax.json.JsonObject

abstract class ModelCompanion<M : Model<M, D>, D> {
    // create data object from instance
    abstract fun createModel(): M
    abstract fun <D> createData(model: M): D

    val toModel: (Row) -> M = { row ->
        createModel().load(row)
    }

    val toData: (Row) -> D = { row ->
        createData(toModel(row))
    }

    val toJson: (Row) -> JsonObject = { row ->
        toModel(row).toJson()
    }
}
