package com.vladsch.kotlin.jdbc

interface DbEntityExtractor {
    fun getExtractEntityNameRegEx(entityTypeName: String): Regex
    fun cleanEntityScript(entity: DbEntity, createScript: String): String
    fun getDropEntitySql(entityType: String, entityName: String): String
    fun getShowEntitySql(entity: DbEntity, entityName: String): String
    fun getListEntitiesSql(entity: DbEntity, schema: String): String
}
