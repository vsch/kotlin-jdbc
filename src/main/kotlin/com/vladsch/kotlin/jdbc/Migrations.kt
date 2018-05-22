package com.vladsch.kotlin.jdbc

import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileWriter

class Migrations(val session: Session, val dbEntityExtractor: DbEntityExtractor, val resourceClass: Class<*>) {
    companion object {
        private val logger = LoggerFactory.getLogger(Migrations::class.java)!!
    }

    fun getVersions(): List<String> {
        val files = getResourceFiles(resourceClass, "/db").filter { it.matches("^(V\\d+(?:_\\d+(?:_\\d+(?:_.*)?)?)?)$".toRegex()) }.map { it.toUpperCase() }.sorted()
        return files
    }

    fun getLatestVersion(): String {
        val versions = getVersions()
        return if (versions.isEmpty()) "V0_0_0" else versions.last();
    }

    fun getEntityQuery(dbEntity: DbEntity): SqlQuery {
        val defaultDb = session.connection.catalog
        val entityQuery = sqlQuery(dbEntityExtractor.getListEntitiesSql(dbEntity, defaultDb));
        return entityQuery
    }

    fun getDbEntities(dbEntity: DbEntity): List<String> {
        val entityQuery = getEntityQuery(dbEntity)
        val entities = session.list(entityQuery) { it.string(1) }
        return entities;
    }

    fun dumpTables(dbDir: File, dbVersion: String) {
        val entity = DbEntity.TABLE
        val tablesDir = entity.getEntityDirectory(dbDir, dbVersion, true)
        val tableFiles = entity.getEntityFiles(tablesDir)

        // delete all existing table sql files
        tableFiles.forEach {
            val file = File(it)
            file.delete()
        }

        val tables = getDbEntities(entity)

        for (table in tables) {
            val tableFile = entity.getEntityFile(tablesDir, table)
            val tableSql = sqlQuery(dbEntityExtractor.getShowEntitySql(entity, table))
            val tableCreate = session.first(tableSql) {
                it.string(2)
            }

            if (tableCreate != null) {
                // remove auto increment start value
                val createScript = dbEntityExtractor.cleanEntityScript(entity, tableCreate)
                val tableWriter = FileWriter(tableFile)
                tableWriter.write(createScript)
                tableWriter.flush()
                tableWriter.close()
            }
        }
    }

    /**
     * Create all tables which exist in tables snapshot but not in the database
     *
     * Creating new tables does not use migrations and uses snapshot instead
     *
     * @param dbVersion String?
     */
    fun createTables(migration: MigrationSession) {
        val entity = DbEntity.TABLE

        // may need to create table directory
        val tables = getDbEntities(entity)
        val tableSet = tables.map { it.toLowerCase() }.toSet()
        val entities = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, migration.version)

        runEntitiesSql(migration, entities, "Creating table", null) { !tableSet.contains(it) }
    }

    private fun runEntitiesSql(migration: MigrationSession, entities: Map<String, DbEntity.EntityScript>, opType: String, dropEntityIfExists: String? = null, excludeFilter: ((String) -> Boolean)? = null) {
        for ((entityName, entityEntry) in entities) {
            if (excludeFilter == null || !excludeFilter.invoke(entityName)) {
                val entityRealName = entityEntry.entityName
                val entityFile = entityEntry.entityFileName
                val entitySqlContents = entityEntry.entitySql

                val query = sqlQuery(entitySqlContents)
                logger.info("$opType $entityRealName from $entityFile")
                migration.invokeWith { session ->
                    migration.insertMigrationAfter(entityFile, entitySqlContents) {
                        if (dropEntityIfExists != null) {
                            session.execute(sqlQuery(dbEntityExtractor.getDropEntitySql(dropEntityIfExists, entityRealName)))
                        }
                        session.execute(query)
                    }
                }
            }
        }
    }

    fun updateEntities(dbEntity: DbEntity, migration: MigrationSession, excludeFilter: ((String) -> Boolean)? = null) {
        // may need to create table directory
        val entities = dbEntity.getEntityResourceScripts(resourceClass, dbEntityExtractor, migration.version)
        runEntitiesSql(migration, entities, "Update ${dbEntity.displayName}", if (dbEntity == DbEntity.TABLE) null else dbEntity.dbEntity, excludeFilter)
    }

    fun initMigrations(dbVersion: String? = null): MigrationSession {
        val entity = DbEntity.TABLE

        val tables = getDbEntities(entity)
        val useDbVersion = dbVersion ?: getLatestVersion()

        val dbTableResourceDir = entity.getEntityResourceDirectory(useDbVersion)
        val tableEntities = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, useDbVersion)
        val migration: MigrationSession

        if (tables.filter { it.toLowerCase() == "migrations" }.isEmpty()) {
            // create the table
            val scriptName: String
            val scriptSql: String
            migration = MigrationSession(1, useDbVersion, this)

            if (tableEntities.contains("migrations")) {
                // run the file found for the version
                val tableEntry = tableEntities["migrations"]!!;
                logger.info("Creating migrations table from ${dbTableResourceDir.path}/${tableEntry.entityFileName}")

                scriptName = tableEntry.entityFileName
                scriptSql = tableEntry.entitySql
            } else {
                scriptSql = migration.createTableSql
                scriptName = "<internal create migration table>"
            }


            migration.invokeWith { session ->
                migration.insertMigrationAfter(scriptName, scriptSql) {
                    session.execute(sqlQuery(scriptSql))
                }
            }
        } else {
            val sqlQuery = sqlQuery("SELECT MAX(batch_id) FROM migrations")
            val batchId = session.first(sqlQuery) {
                it.int(1)
            } ?: 0

            migration = MigrationSession(batchId + 1, useDbVersion, this)
        }

        return migration
    }

    /**
     * Execute db command
     *
     * @param args Array<String>
     *
     *     init                     - initialize migrations table and migrate all to given version or latest version
     *
     *     path                     - path to resources/db directory
     *     version versionID        - migrate to latest version and compare to snapshots
     *     migrate                  - migrate to given version or to latest version and validate-all
     *     rollback                 - rollback to given version or to previous version
     *
     *     dump-all                 - dump database structure all: tables, views, triggers, functions and stored procedures
     *     dump-tables              - dump database tables
     *     dump-views               - dump database views
     *     dump-functions           - dump database functions
     *     dump-procedures          - dump database stored procedures
     *     dump-triggers            - dump database triggers
     *
     *     update-all               - update all: functions, views, procedures, triggers
     *     update-procedures
     *     update-procs             - update stored procedures
     *
     *     update-functions
     *     update-funcs             - update functions
     *
     *     update-triggers          - update triggers
     *
     *     update-views             - update views
     *
     *     validate-all             - validate db version content match for all
     *     validate-tables          - validate db version tables
     *     validate-views           - validate db version views
     *     validate-functions       - validate db version functions
     *     validate-procedures      - validate db version procedures
     *     validate-triggers        - validate db version triggers
     */
    fun dbCommand(args: Array<String>) {
        var dbVersion: String? = null
        var dbPath: File = File(System.getProperty("user.dir"))

        var migration: MigrationSession? = null

        session.transaction { tx ->
            try {
                var i = 0
                while (i < args.size) {
                    val option = args[i++]
                    when (option) {
                        "init" -> {
                            if (migration != null) {
                                throw IllegalArgumentException("db init command must be first executed command")
                            }
                            migration = initMigrations(dbVersion)
                        }

                        "migrate" -> {
                            // here need to apply up migrations from current version to given version or latest in version sorted order
                        }

                        "version" -> {
                            if (args.size <= i) {
                                throw IllegalArgumentException("version option requires a version argument")
                            }
                            if (dbVersion != null) {
                                throw IllegalArgumentException("db version command must come before commands that require version")
                            }
                            val versions = getVersions()
                            val version = args[i++]
                            if (!versions.contains(version.toUpperCase())) {
                                throw IllegalArgumentException("version $version does not exist in classpath '/db'")
                            }
                            dbVersion = version
                        }

                        "path" -> {
                            if (args.size < i) {
                                throw IllegalArgumentException("path option requires a path argument")
                            }
                            val path = args[i++]
                            val pathDir = File(path).ensureExistingDirectory("path")
                            dbPath = pathDir
                        }

                        "rollback" -> {
                            // here need to apply down migrations from current version to given version or if none given then rollback the last batch which was not rolled back
                        }

                        "dump-tables" -> {
                            if (dbVersion == null) dbVersion = getLatestVersion()
                            dumpTables(dbPath, dbVersion!!)
                        }

                        "update-all" -> {
                            if (migration == null) migration = initMigrations(dbVersion)

                            updateEntities(DbEntity.FUNCTION, migration!!)
                            updateEntities(DbEntity.VIEW, migration!!)
                            updateEntities(DbEntity.TRIGGER, migration!!)
                            updateEntities(DbEntity.PROCEDURE, migration!!)
                        }

                        "update-procedures", "update-procs" -> {
                            if (migration == null) migration = initMigrations(dbVersion)
                            updateEntities(DbEntity.PROCEDURE, migration!!)
                        }

                        "update-functions", "update-funcs" -> {
                            if (migration == null) migration = initMigrations(dbVersion)

                            updateEntities(DbEntity.FUNCTION, migration!!)
                        }

                        "update-views" -> {
                            if (migration == null) migration = initMigrations(dbVersion)

                            updateEntities(DbEntity.VIEW, migration!!)
                        }

                        "update-triggers" -> {
                            if (migration == null) migration = initMigrations(dbVersion)

                            updateEntities(DbEntity.TRIGGER, migration!!)
                        }

                        else -> {
                            throw IllegalArgumentException("db option $option is not recognized")
                        }
                    }
                }
            } catch (e: Exception) {
                val migrationSession = migration
                if (migrationSession != null) {
                    session.transaction { tx2 ->
                        val migrationSql = migrationSession.getMigrationSql(
                            migrationSession.lastScriptName ?: "",
                            migrationSession.lastScriptSql ?: ""
                        ).inParams("lastProblem" to e.message)

                        tx2.execute(migrationSql)
                        tx2.commit()
                    }
                }

                // re-throw exception
                throw e
            }

            tx.commit()
        }
    }
}

