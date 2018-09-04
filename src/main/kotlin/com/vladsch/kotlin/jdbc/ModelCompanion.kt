package com.vladsch.kotlin.jdbc

import javax.json.JsonArray
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

    fun listQuery(whereClause:String, vararg params: Pair<String, Any?>): SqlQuery {
        return Model.listQuery(tableName, whereClause, params)
    }

    fun listQuery(whereClause:String, params: Map<String, Any?>): SqlQuery {
        return Model.listQuery(tableName, whereClause, params)
    }

    fun list(session: Session, vararg params: Pair<String, Any?>): List<D> {
        return session.list(Model.listQuery(tableName, params), toData)
    }

    fun list(session: Session, params: Map<String, Any?>):  List<D> {
        return session.list(Model.listQuery(tableName, params), toData)
    }

    fun jsonArray(session: Session, vararg params: Pair<String, Any?>): JsonArray {
        return session.jsonArray(Model.listQuery(tableName, params), toJson)
    }

    fun jsonArray(session: Session, params: Map<String, Any?>):  JsonArray {
        return session.jsonArray(Model.listQuery(tableName, params), toJson)
    }

    fun list(session: Session, whereClause:String, vararg params: Pair<String, Any?>): List<D> {
        return session.list(Model.listQuery(tableName, whereClause, params), toData)
    }

    fun list(session: Session, whereClause:String, params: Map<String, Any?>):  List<D> {
        return session.list(Model.listQuery(tableName, whereClause, params), toData)
    }

    fun jsonArray(session: Session, whereClause:String, vararg params: Pair<String, Any?>): JsonArray {
        return session.jsonArray(Model.listQuery(tableName, whereClause, params), toJson)
    }

    fun jsonArray(session: Session, whereClause:String, params: Map<String, Any?>):  JsonArray {
        return session.jsonArray(Model.listQuery(tableName, whereClause, params), toJson)
    }
}
