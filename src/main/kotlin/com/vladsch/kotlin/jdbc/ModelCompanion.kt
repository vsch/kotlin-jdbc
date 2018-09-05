package com.vladsch.kotlin.jdbc

import javax.json.JsonArray
import javax.json.JsonObject

abstract class ModelCompanion<M:Model<M>, D> {
    abstract val tableName:String
    abstract fun createModel(quote:String?): M
    abstract fun createData(model:M): D

    val toModel: (Row) -> M = toModel()
    fun toModel(quote: String? = null): (Row) -> M = { row ->
        createModel(quote).load(row)
    }

    val toData: (Row) -> D = toData()
    fun toData(quote: String? = null): (Row) -> D = { row ->
        createData(createModel(quote).load(row))
    }

    val toJson: (Row) -> JsonObject = toJson()
    fun toJson(quote: String? = null): (Row) -> JsonObject = { row ->
        createModel(quote).load(row).toJson()
    }

    fun listQuery(vararg params: Pair<String, Any?>, quote:String = ModelProperties.databaseQuoting): SqlQuery {
        return Model.listQuery(tableName, params, quote)
    }

    fun listQuery(params: Map<String, Any?>, quote:String = ModelProperties.databaseQuoting): SqlQuery {
        return Model.listQuery(tableName, params, quote)
    }

    fun listQuery(whereClause:String, vararg params: Pair<String, Any?>, quote:String = ModelProperties.databaseQuoting): SqlQuery {
        return Model.listQuery(tableName, whereClause, params, quote)
    }

    fun listQuery(whereClause:String, params: Map<String, Any?>, quote:String = ModelProperties.databaseQuoting): SqlQuery {
        return Model.listQuery(tableName, whereClause, params, quote)
    }

    fun list(session: Session, vararg params: Pair<String, Any?>, quote:String = ModelProperties.databaseQuoting): List<D> {
        return session.list(Model.listQuery(tableName, params, quote), toData(quote))
    }

    fun list(session: Session, params: Map<String, Any?>, quote:String = ModelProperties.databaseQuoting):  List<D> {
        return session.list(Model.listQuery(tableName, params, quote), toData(quote))
    }

    fun jsonArray(session: Session, vararg params: Pair<String, Any?>, quote:String = ModelProperties.databaseQuoting): JsonArray {
        return session.jsonArray(Model.listQuery(tableName, params, quote), toJson(quote))
    }

    fun jsonArray(session: Session, params: Map<String, Any?>, quote:String = ModelProperties.databaseQuoting):  JsonArray {
        return session.jsonArray(Model.listQuery(tableName, params, quote), toJson(quote))
    }

    fun list(session: Session, whereClause:String, vararg params: Pair<String, Any?>, quote:String = ModelProperties.databaseQuoting): List<D> {
        return session.list(Model.listQuery(tableName, whereClause, params, quote), toData(quote))
    }

    fun list(session: Session, whereClause:String, params: Map<String, Any?>, quote:String = ModelProperties.databaseQuoting):  List<D> {
        return session.list(Model.listQuery(tableName, whereClause, params, quote), toData(quote))
    }

    fun jsonArray(session: Session, whereClause:String, vararg params: Pair<String, Any?>, quote:String = ModelProperties.databaseQuoting): JsonArray {
        return session.jsonArray(Model.listQuery(tableName, whereClause, params, quote), toJson(quote))
    }

    fun jsonArray(session: Session, whereClause:String, params: Map<String, Any?>, quote:String = ModelProperties.databaseQuoting):  JsonArray {
        return session.jsonArray(Model.listQuery(tableName, whereClause, params, quote), toJson(quote))
    }
}
