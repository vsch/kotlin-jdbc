package com.vladsch.kotlin.jdbc

import org.intellij.lang.annotations.Language
import java.time.LocalDateTime

class MigrationSession(val batchId: Int, val version: String, val migrations: Migrations) {

    data class Migration(
        val migration_id: Int,
        val version: String,
        val batch_id: Int,
        val applied_at: LocalDateTime,
        val migration_type: Int?,
        val script_name: String,
        val script_sql: String,
        val rolled_back_id: Int?,
        val last_problem: String?
    ) {

        companion object {
            @JvmStatic
            val toModel: (Row) -> Migration = { row ->
                Migration(
                    row.int("migration_id"),
                    row.string("version"),
                    row.int("batch_id"),
                    row.localDateTime("applied_at"),
                    row.intOrNull("migration_type"),
                    row.string("script_name"),
                    row.string("script_sql"),
                    row.intOrNull("rolled_back_id"),
                    row.stringOrNull("last_problem")
                )
            }
        }
    }

    @Language("SQL")
    val createTableSql = """
CREATE TABLE `migrations` (
  `migration_id`   INT(11)     NOT NULL AUTO_INCREMENT,
  `version`        VARCHAR(32) NOT NULL,
  `batch_id`       INT(11)     NOT NULL,
  `applied_at`     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `migration_type` TINYINT(1),
  `rolled_back_id` INT(11), # migration_id of roll back script
  `script_name`    VARCHAR(128),
  `script_sql`     MEDIUMTEXT,
  `last_problem`   MEDIUMTEXT,
  PRIMARY KEY (`migration_id`)
) DEFAULT CHARSET = utf8
"""

    @Language("SQL")
    val migrationSql = """
INSERT INTO migrations (
  version,
  batch_id,
  script_name,
  script_sql,
  migration_type,
  last_problem
) VALUES (
  :version,
  :batchId,
  :scriptName,
  :scriptSql,
  :migrationType,
  :lastProblem
)
"""
    var lastScriptName: String? = null
    var lastScriptSql: String? = null

    fun <T : Any> invokeWith(action: (Session) -> T?): T? {
        return action.invoke(migrations.session)
    }

    fun getMigrationSql(scriptName: String, scriptSql: String, migrationType:Int? = null): SqlQuery {
        lastScriptName = scriptName
        lastScriptSql = scriptSql
        return sqlQuery(migrationSql, mapOf("version" to version, "batchId" to batchId, "scriptName" to scriptName, "scriptSql" to scriptSql, "migrationType" to migrationType))
    }

    fun insertUpMigrationAfter(scriptName: String, scriptSql: String, action: () -> Unit) {
        val sqlQuery = getMigrationSql(scriptName, scriptSql, 1)

        action.invoke()
        val transId = migrations.session.updateGetId(sqlQuery)

        // now need to mark all rollbacks of this script that they were reversed
        val rollBackScriptName = DbEntity.ROLLBACK.addSuffix( DbEntity.MIGRATION.removeSuffix(scriptName))
        val updateSql = sqlQuery("""
UPDATE migrations SET rolled_back_id = :transId WHERE (script_name = :scriptName OR script_name LIKE :scriptNameLike) AND version = :version AND migration_type = -1 AND rolled_back_id IS NULL
""", mapOf("transId" to transId, "version" to version, "scriptName" to rollBackScriptName, "scriptNameLike" to "$rollBackScriptName[%]"))
        migrations.session.execute(updateSql)
    }

    fun insertDownMigrationAfter(scriptName: String, scriptSql: String, action: () -> Unit) {
        val sqlQuery = getMigrationSql(scriptName, scriptSql, -1)

        action.invoke()
        val transId = migrations.session.updateGetId(sqlQuery)

        // now need to mark all rollbacks of this script that they were reversed
        val rollBackScriptName = DbEntity.MIGRATION.addSuffix( DbEntity.ROLLBACK.removeSuffix(scriptName))
        val updateSql = sqlQuery("""
UPDATE migrations SET rolled_back_id = :transId WHERE (script_name = :scriptName OR script_name LIKE :scriptNameLike) AND version = :version AND migration_type = 1 AND rolled_back_id IS NULL
""", mapOf("transId" to transId, "version" to version, "scriptName" to rollBackScriptName, "scriptNameLike" to "$rollBackScriptName[%]"))
        migrations.session.execute(updateSql)
    }

    fun insertMigrationAfter(scriptName: String, scriptSql: String, action: () -> Unit) {
        val sqlQuery = getMigrationSql(scriptName, scriptSql)

        action.invoke()
        migrations.session.execute(sqlQuery)
    }

    fun getVersionBatches(): List<Migration> {
        return migrations.session.list(sqlQuery("""
SELECT * FROM migrations
WHERE rolled_back_id IS NULL AND last_problem IS NULL
ORDER BY migration_id ASC
"""), Migration.toModel)
    }

    fun getVersionBatchesNameMap(): Map<String, Migration> {
        val query = sqlQuery("""
SELECT * FROM migrations
WHERE rolled_back_id IS NULL AND last_problem IS NULL
ORDER BY migration_id ASC
""")
        val keyExtractor: (Row) -> String = { row ->
            row.string("script_name")
        }

        return migrations.session.hashMap(query, keyExtractor, Migration.toModel)
    }

    fun getAllVersionBatches(): List<Migration> {
        return migrations.session.list(sqlQuery("""
SELECT * FROM migrations
ORDER BY migration_id ASC
"""), Migration.toModel)
    }

    fun withVersion(version: String): MigrationSession {
        return MigrationSession(batchId, version, migrations)
    }
}

