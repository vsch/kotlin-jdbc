package com.vladsch.kotlin.jdbc

object MySqlEntityExtractor : DbEntityExtractor {
    override fun getExtractEntityNameRegEx(entity: DbEntity): Regex? {
        return when (entity) {
            DbEntity.MIGRATION -> null
            DbEntity.ROLLBACK -> null
            else-> "^\\s*CREATE ${entity.dbEntity}\\s+(`[\\u0001-\\u005F\\u0061-\\uFFFF]+`|[0-9a-z_A-Z$\\u0080-\\uFFFF]+)".toRegex(RegexOption.IGNORE_CASE)
//            DbEntity.FUNCTION -> createScript
//            DbEntity.PROCEDURE -> createScript
//            DbEntity.TABLE -> createScript.replace(createTableCleanRegex, "")
//            DbEntity.TRIGGER -> createScript
//            DbEntity.VIEW -> createScript
        }
    }

    val createTableCleanRegex = "(?:\\s+ENGINE\\s*=\\s*[a-zA-Z0-9]+)?(?:\\s+AUTO_INCREMENT\\s*=\\s*\\d+)?".toRegex();

    override fun cleanEntityScript(entity: DbEntity, createScript: String): String {
        return when (entity) {
            DbEntity.FUNCTION -> createScript
            DbEntity.PROCEDURE -> createScript
            DbEntity.TABLE -> createScript.replace(createTableCleanRegex, "")
            DbEntity.TRIGGER -> createScript
            DbEntity.VIEW -> createScript
            DbEntity.MIGRATION -> createScript
            DbEntity.ROLLBACK -> createScript
        }
    }

    override fun getDropEntitySql(entityType: String, entityName: String): String {
        return "DROP $entityType IF EXISTS `$entityName`"
    }

    override fun getShowEntitySql(entity: DbEntity, entityName: String): String {
        return when (entity) {
            DbEntity.FUNCTION -> ""
            DbEntity.PROCEDURE -> ""
            DbEntity.TABLE -> "SHOW CREATE TABLE `$entityName`"
            DbEntity.TRIGGER -> ""
            DbEntity.VIEW -> ""
            DbEntity.MIGRATION -> ""
            DbEntity.ROLLBACK -> ""
        }
    }

    override fun getListEntitiesSql(entity: DbEntity, schema: String): String {
        return when (entity) {
            DbEntity.FUNCTION -> "SELECT routine_name FROM information_schema.ROUTINES WHERE routine_schema = '$schema' AND routine_type == 'FUNCTION'"
            DbEntity.PROCEDURE -> "SELECT routine_name FROM information_schema.ROUTINES WHERE routine_schema = '$schema' AND routine_type == 'PROCEDURE'"
            DbEntity.TABLE -> "SELECT table_name FROM information_schema.TABLES WHERE table_schema = '$schema'"
            DbEntity.TRIGGER -> "SELECT trigger_name FROM information_schema.TRIGGERS WHERE trigger_schema = '$schema'"
            DbEntity.VIEW -> "SELECT table_name FROM information_schema.VIEWS WHERE table_schema = '$schema'"
            DbEntity.MIGRATION -> ""
            DbEntity.ROLLBACK -> ""
        }
    }
}
