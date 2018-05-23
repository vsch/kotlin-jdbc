package com.vladsch.kotlin.jdbc

interface DbEntityExtractor {
    fun getExtractEntityNameRegEx(entity: DbEntity): Regex?
    fun getDropEntitySql(entityType: String, entityName: String): String
    fun getShowEntitySql(entity: DbEntity, entityName: String): String
    fun getListEntitiesSql(entity: DbEntity, schema: String): String
    fun entityScriptFixer(entity: DbEntity, session: Session): DbEntityFixer
    fun getDbEntities(dbEntity: DbEntity, session: Session): List<String>
    fun getEntityQuery(dbEntity: DbEntity, session: Session): SqlQuery
    fun getDropEntitySql(entity: DbEntity, entityName: String): String
}
