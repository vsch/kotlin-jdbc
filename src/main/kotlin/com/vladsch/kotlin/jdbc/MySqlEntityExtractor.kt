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


    class TableScriptFixer(tables:List<String>) : DbEntityCaseFixer(tables) {
        val createCleanRegex = "(?:\\s+ENGINE\\s*=\\s*[a-zA-Z0-9]+)|(?:\\s+AUTO_INCREMENT\\s*=\\s*\\d+)".toRegex();

        override fun addRegExPrefix(sb: StringBuilder) {
            sb.append("\\s+REFERENCES\\s+")
            super.addRegExPrefix(sb)
        }

        override fun addRegExSuffix(sb: StringBuilder) {
            super.addRegExSuffix(sb)
        }

        override fun addEntityNameRegEx(sb: StringBuilder, entityName: String) {
            super.addEntityNameRegEx(sb, entityName)
        }

        override fun cleanScript(createScript: String): String {
            val cleanedScript = createScript.replace(createCleanRegex, "")
            return super.cleanScript(cleanedScript)
        }
    }

    class ViewScriptFixer(tables:List<String>) : DbEntityCaseFixer(tables) {
        val createCleanRegex = "(?:\\s+ENGINE\\s*=\\s*[a-zA-Z0-9]+)|(?:\\s+AUTO_INCREMENT\\s*=\\s*\\d+)".toRegex();

        override fun addRegExPrefix(sb: StringBuilder) {
            sb.append("\\s+REFERENCES\\s+")
            super.addRegExPrefix(sb)
        }

        override fun addRegExSuffix(sb: StringBuilder) {
            super.addRegExSuffix(sb)
        }

        override fun addEntityNameRegEx(sb: StringBuilder, entityName: String) {
            super.addEntityNameRegEx(sb, entityName)
        }

        override fun cleanScript(createScript: String): String {
            val cleanedScript = createScript.replace(createCleanRegex, "")
            return super.cleanScript(cleanedScript)
        }
    }

    override fun getEntityQuery(dbEntity: DbEntity, session: Session): SqlQuery {
        val defaultDb = session.connection.catalog
        val entityQuery = sqlQuery(getListEntitiesSql(dbEntity, defaultDb));
        return entityQuery
    }

    override fun getDbEntities(dbEntity: DbEntity, session: Session): List<String> {
        val entityQuery = getEntityQuery(dbEntity, session)
        val entities = session.list(entityQuery) { it.string(1) }
        return entities;
    }

    override fun entityScriptFixer(entity: DbEntity, session: Session): DbEntityFixer {
        return when (entity) {
            DbEntity.FUNCTION -> DbEntityFixer.NULL
            DbEntity.PROCEDURE -> DbEntityFixer.NULL
            DbEntity.TABLE -> {
                val tables = getDbEntities(entity, session)
                return TableScriptFixer(tables)
            }
            DbEntity.TRIGGER -> DbEntityFixer.NULL
            DbEntity.VIEW -> DbEntityFixer.NULL
            DbEntity.MIGRATION -> DbEntityFixer.NULL
            DbEntity.ROLLBACK -> DbEntityFixer.NULL
        }
    }

    override fun getDropEntitySql(entityType: String, entityName: String): String {
        return "DROP $entityType IF EXISTS `$entityName`"
    }

    override fun getDropEntitySql(entity: DbEntity, entityName: String): String {
        return getDropEntitySql(entity.dbEntity, entityName)
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
            DbEntity.FUNCTION -> "SELECT routine_name FROM information_schema.ROUTINES WHERE routine_schema = '$schema' AND routine_type = 'FUNCTION'"
            DbEntity.PROCEDURE -> "SELECT routine_name FROM information_schema.ROUTINES WHERE routine_schema = '$schema' AND routine_type = 'PROCEDURE'"
            DbEntity.TABLE -> "SELECT table_name FROM information_schema.TABLES WHERE table_schema = '$schema' AND TABLE_TYPE = 'BASE TABLE'"
            DbEntity.TRIGGER -> "SELECT trigger_name FROM information_schema.TRIGGERS WHERE trigger_schema = '$schema'"
            DbEntity.VIEW -> "SELECT table_name FROM information_schema.VIEWS WHERE table_schema = '$schema'"
            DbEntity.MIGRATION -> ""
            DbEntity.ROLLBACK -> ""
        }
    }
}
