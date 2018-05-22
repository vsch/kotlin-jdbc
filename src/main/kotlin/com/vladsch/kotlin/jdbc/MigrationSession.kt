package com.vladsch.kotlin.jdbc

import org.intellij.lang.annotations.Language

class MigrationSession(val batchId: Int, val version: String, val migrations: Migrations) {
    @Language("SQL")
    val createTableSql = """
CREATE TABLE `migrations` (
  `migration_id`   INT(11)     NOT NULL AUTO_INCREMENT,
  `version`        VARCHAR(32) NOT NULL,
  `batch_id`       INT(11)     NOT NULL,
  `applied_at`     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
  ON UPDATE CURRENT_TIMESTAMP,
  `migration_type` TINYINT(1),
  `script_name`    VARCHAR(128),
  `script_sql`     MEDIUMTEXT,
  `rolled_back_id` INT(11), # migration_id of roll back script
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

    fun getMigrationSql(scripName: String, scriptSql: String): SqlQuery {
        lastScriptName = scripName
        lastScriptSql = scriptSql
        return sqlQuery(migrationSql, mapOf("version" to version, "batchId" to batchId, "scriptName" to scripName, "scriptSql" to scriptSql))
    }

    fun insertMigrationAfter(scripName: String, scriptSql: String, action: () -> Unit) {
        val sqlQuery = getMigrationSql(scripName, scriptSql)

        action.invoke()
        migrations.session.execute(sqlQuery)
    }
}
