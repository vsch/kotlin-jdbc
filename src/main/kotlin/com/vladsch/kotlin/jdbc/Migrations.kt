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

    fun getPreviousVersion(version: String): String? {
        val versions = getVersions();
        var lastVersion: String? = null
        for (availVersion in versions) {
            val versionCompare = version.versionCompare(availVersion)
            if (versionCompare <= 0) break
            lastVersion = availVersion
        }
        return lastVersion
    }

    fun getLatestVersion(): String {
        val versions = getVersions()
        return if (versions.isEmpty()) "V0_0_0" else versions.last();
    }

    private fun doForEachEntity(entity: DbEntity, tablesDir: File, entities: List<String>, consumer: (tableFile: File, tableScript: String) -> Unit) {
        val entityFixer = dbEntityExtractor.entityScriptFixer(entity, session)
        for (entityName in entities) {
            val entityFile = entity.getEntityFile(tablesDir, entityName)
            val entitySql = sqlQuery(dbEntityExtractor.getShowEntitySql(entity, entityName))
            val entityCreate = session.first(entitySql) {
                it.string(2)
            }

            if (entityCreate != null) {
                // remove auto increment start value
                val fixedCaseSql = entityFixer.cleanScript(entityCreate)
                consumer.invoke(entityFile, fixedCaseSql)
            }
        }
    }

    fun forEachEntity(entity: DbEntity, dbDir: File, dbVersion: String, consumer: (tableFile: File, tableScript: String) -> Unit) {
        val tablesDir = entity.getEntityDirectory(dbDir, dbVersion, true)
        val tables = dbEntityExtractor.getDbEntities(entity, session)
        doForEachEntity(DbEntity.TABLE, tablesDir, tables, consumer)
    }

    fun forEachEntity(entity: DbEntity, dbVersion: String, consumer: (tableFile: File, tableScript: String) -> Unit) {
        val tablesDir = entity.getEntityResourceDirectory(dbVersion)
        val tables = dbEntityExtractor.getDbEntities(entity, session)
        doForEachEntity(DbEntity.TABLE, tablesDir, tables, consumer)
    }

    fun forEachEntityFile(dbDir: File, dbVersion: String, consumer: (tableFile: File) -> Unit, entity: DbEntity) {
        val tablesDir = entity.getEntityDirectory(dbDir, dbVersion, true)
        val tableFiles = entity.getEntityFiles(tablesDir)

        // delete all existing table sql files
        tableFiles.forEach {
            val file = File(it)
            consumer.invoke(file)
        }
    }

    fun forEachEntityResourceFile(entity: DbEntity, dbVersion: String, consumer: (tableFile: File) -> Unit) {
        val tablesDir = entity.getEntityResourceDirectory(dbVersion)
        val tableFiles = entity.getEntityResourceFiles(resourceClass, dbVersion)

        // delete all existing table sql files
        tableFiles.forEach {
            val file = tablesDir + it
            consumer.invoke(file)
        }
    }

    fun dumpTables(dbDir: File, dbVersion: String) {
        val migrationsFileName = DbEntity.TABLE.addSuffix("migrations")
        forEachEntityFile(dbDir, dbVersion, { tableFile ->
            tableFile.delete()
        }, DbEntity.TABLE)

        forEachEntity(DbEntity.TABLE, dbDir, dbVersion, { tableFile, tableScript ->
            if (tableFile.name != migrationsFileName) {
                val tableWriter = FileWriter(tableFile)
                tableWriter.write(tableScript)
                tableWriter.flush()
                tableWriter.close()
            }
        })
    }

    fun validateTables(dbDir: File, dbVersion: String) {
        val tableSet = HashSet<String>()
        val migrationsFileName = DbEntity.TABLE.addSuffix("migrations")
        forEachEntity(DbEntity.TABLE, dbDir, dbVersion, { tableFile, tableScript ->
            if (tableFile.name != migrationsFileName) {
                tableSet.add(tableFile.path)

                if (tableFile.exists()) {
                    val tableSql = getFileContent(tableFile)
                    if (tableSql.trim() != tableScript.trim()) {
                        logger.error("Table validation failed for ${tableFile.path}, database and file differ")
                    }
                } else {
                    logger.error("Table validation failed for ${tableFile.path}, file is missing")
                }
            }
        })

        forEachEntityFile(dbDir, dbVersion, { tableFile ->
            if (tableFile.name != migrationsFileName) {
                if (!tableSet.contains(tableFile.path)) {
                    logger.error("Table validation failed for ${tableFile.path}, no database table for file")
                }
            }
        }, DbEntity.TABLE)
    }

    fun doesResourceExist(resourcePath: String): Boolean {
        try {
            resourceClass.getResource(resourcePath)
            return true
        } catch (e: Exception) {
            return false
        }
    }

    fun validateTableResourceFiles(dbVersion: String, errorAppendable: Appendable? = null) {
        val tableSet = HashSet<String>()
        val migrationsFileName = DbEntity.TABLE.addSuffix("migrations")
        forEachEntity(DbEntity.TABLE, dbVersion, { tableFile, tableScript ->
            if (tableFile.name != migrationsFileName) {
                tableSet.add(tableFile.path)

                if (doesResourceExist(tableFile.path)) {
                    val tableSql = getResourceAsString(resourceClass, tableFile.path)
                    if (tableSql.trim() != tableScript.trim()) {
                        val s = "Table validation failed for ${tableFile.path}, database and resource differ"
                        logger.error(s)
                        errorAppendable?.appendln(s)
                    }
                } else {
                    val s = "Table validation failed for ${tableFile.path}, resource is missing"
                    logger.error(s)
                    errorAppendable?.appendln(s)
                }
            }
        })

        forEachEntityResourceFile(DbEntity.TABLE, dbVersion, { tableFile ->
            if (tableFile.name != migrationsFileName) {
                if (!tableSet.contains(tableFile.path)) {
                    val s = "Table validation failed for ${tableFile.path}, no database table for resource"
                    logger.error(s)
                    errorAppendable?.appendln(s)
                }
            }
        })
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
        val tables = dbEntityExtractor.getDbEntities(entity, session)
        val tableSet = tables.map { it.toLowerCase() }.toSet()
        val entities = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, migration.version)

        runEntitiesSql(migration, entities, "Creating table", null) { tableSet.contains(it.toLowerCase()) }
    }

    private fun runEntitiesSql(migration: MigrationSession, entities: Map<String, DbEntity.EntityScript>, opType: String, dropEntityIfExists: String? = null, excludeFilter: ((String) -> Boolean)? = null) {
        for ((entityName, entityEntry) in entities) {
            if (excludeFilter == null || !excludeFilter.invoke(entityName)) {
                val entityRealName = entityEntry.entityName
                val entityFile = entityEntry.entityResourcePath
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

    fun updateEntities(entity: DbEntity, migration: MigrationSession, excludeFilter: ((String) -> Boolean)? = null) {
        // may need to create table directory
        val entities = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, migration.version)
        runEntitiesSql(migration, entities, "Update ${entity.displayName}", if (entity == DbEntity.TABLE) null else entity.dbEntity, excludeFilter)

        // delete all which do not exist in files
        val dbEntities = dbEntityExtractor.getDbEntities(entity, session)
        dbEntities.forEach { dbEntityName ->
            if (!entities.containsKey(dbEntityName.toLowerCase())) {
                val dropEntitySql = dbEntityExtractor.getDropEntitySql(entity, dbEntityName)
                logger.info("Dropping ${entity.displayName} $dbEntityName")
                session.execute(sqlQuery(dropEntitySql))
            }
        }
    }

    /**
     * Take the latest actual applied migration or rollback version, failing that take <migrate> or <rollback> entry version
     */
    fun getCurrentVersion(): String? {
        return session.first(sqlQuery("""
SELECT version FROM migrations
WHERE rolled_back_id IS NULL AND last_problem IS NULL AND migration_type IS NOT NULL
ORDER BY migration_id DESC
LIMIT 1
""")) { row ->
            row.string(1)
        } ?: session.first(sqlQuery("""
SELECT version FROM migrations
WHERE rolled_back_id IS NULL AND last_problem IS NULL AND script_name IN ('<migrate>', '<rollback>')
ORDER BY migration_id DESC
LIMIT 1
""")) { row ->
            row.string(1)
        }
    }

    fun initMigrations(dbVersion: String? = null): MigrationSession {
        val entity = DbEntity.TABLE

        val migration: MigrationSession
        val tables = dbEntityExtractor.getDbEntities(entity, session)

        if (tables.filter { it.toLowerCase() == "migrations" }.isEmpty()) {
            val useDbVersion = dbVersion ?: getLatestVersion()

            val dbTableResourceDir = entity.getEntityResourceDirectory(useDbVersion)
            val tableEntities = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, useDbVersion)

            // create the table
            val scriptName: String
            val scriptSql: String
            migration = MigrationSession(1, useDbVersion, this)

            if (tableEntities.contains("migrations")) {
                // run the file found for the version
                val tableEntry = tableEntities["migrations"]!!;
                logger.info("Creating migrations table from ${dbTableResourceDir.path}/${tableEntry.entityResourcePath}")

                scriptName = tableEntry.entityResourcePath
                scriptSql = tableEntry.entitySql
            } else {
                scriptSql = migration.createTableSql
                scriptName = "<internal create migration table>"
            }

            // create migration table
            migration.insertMigrationAfter(scriptName, scriptSql) {
                session.execute(sqlQuery(scriptSql))
            }
        } else {
            val sqlQuery = sqlQuery("SELECT MAX(batch_id) FROM migrations")
            val batchId = session.first(sqlQuery) {
                it.int(1)
            } ?: 0
            migration = MigrationSession(batchId + 1, dbVersion ?: getCurrentVersion() ?: getLatestVersion(), this)
        }

        return migration
    }

    fun migrate(migration: MigrationSession) {
        // here need to apply up migrations from current version to given version or latest in version sorted order
        val entity = DbEntity.MIGRATION;
        val currentVersion = getCurrentVersion() ?: "V0_0_0"
        if (currentVersion == null) {
            // no current version, we just update everything to given version
        } else {
            val versionCompare = currentVersion.versionCompare(migration.version)
            if (versionCompare > 0) {
                logger.info("Migrate: requested version ${migration.version} is less than current version $currentVersion, use rollback instead")
                return
            } else if (versionCompare <= 0) {
                // need to run all up migrations from current version which have not been run
                val migrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, currentVersion).values.toList().sortedBy { it.entityResourcePath }

                if (!migrations.isEmpty()) {
                    val appliedMigrations = migration.getVersionBatchesNameMap()

                    migrations.forEach { entityScript ->
                        val migrationScriptPath = entityScript.entityResourcePath

                        if (!appliedMigrations.containsKey(entityScript.entityResourcePath)) {
                            // apply the migration
                            val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                            migration.insertUpMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                                logger.info("Migrate ${entityScript.entityResourcePath}")
                                runBatchScript(entity, migration, migrationScriptPath, appliedMigrations, sqlScript, entityScript)
                            }
                        }
                    }
                } else {
                    logger.debug("Migrate: no migrations in current version $currentVersion")
                }

                if (versionCompare < 0) {
                    // need to run all migrations from later versions up to requested version
                    val versionList = getVersions().filter { it.compareTo(currentVersion) > 0 }.sortedWith(Comparator(String::versionCompare))

                    versionList.forEach { version ->
                        val versionMigrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, version).values.toList().sortedBy { it.entityResourcePath }

                        if (!versionMigrations.isEmpty()) {
                            versionMigrations.forEach { entityScript ->
                                // apply the migration
                                val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                                migration.insertUpMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                                    logger.info("Migrate ${entityScript.entityResourcePath}")
                                    runBatchScript(entity, migration, entityScript.entityResourcePath, null, sqlScript, entityScript)
                                }
                            }
                        } else {
                            logger.debug("Migrate: no up migrations in version $version")
                        }
                    }
                }
            }

        }

        // run all updates from requested version
        updateEntities(DbEntity.FUNCTION, migration)

        // create all tables from current version which do not exist
        createTables(migration)

        // TODO: validate that current db tables and their definition matches the table list

        updateEntities(DbEntity.VIEW, migration)
        updateEntities(DbEntity.TRIGGER, migration)
        updateEntities(DbEntity.PROCEDURE, migration)

        migration.insertMigrationAfter("<migrate>", "") {}
    }

    private fun runBatchScript(
        opType: DbEntity,
        migration: MigrationSession,
        migrationScriptPath: String,
        appliedMigrations: Map<String, MigrationSession.Migration>?,
        sqlScript: String,
        entityScript: DbEntity.EntityScript
    ) {
        val sqlParts = sqlScript.replace(";\n", "\n;").split(';')
        var line = 1
        var index = 0
        sqlParts.forEach { sql ->
            if (!sql.isBlank()) {
                index++
                val partLines = sql.count { it == '\n' }
                val startLine = line
                line += partLines
                val migrationPartName = "$migrationScriptPath[${index}:$startLine-${line - 1}]"
                if (opType == DbEntity.MIGRATION) {
                    if (appliedMigrations == null || !appliedMigrations.containsKey(migrationPartName)) {
                        migration.insertUpMigrationAfter(migrationPartName, sql) {
                            logger.info("Migrate ${entityScript.entityResourcePath} part [${index}:$startLine-${line - 1}]")
                            session.execute(sqlQuery(sql))
                        }
                    }
                } else {
                    if (appliedMigrations == null || appliedMigrations.containsKey(migrationPartName)) {
                        migration.insertDownMigrationAfter(migrationPartName, sql) {
                            logger.info("Rollback ${entityScript.entityResourcePath} part [${index}:$startLine-${line - 1}]")
                            session.execute(sqlQuery(sql))
                        }
                    }
                }
            } else {
                val partLines = sql.count { it == '\n' }
                line += partLines
            }
        }
    }

    fun rollback(migration: MigrationSession) {
        // here need to apply down migrations from current version to given version or if none given then rollback the last batch which was not rolled back
        val entity = DbEntity.ROLLBACK;
        val currentVersion = getCurrentVersion()
        if (currentVersion == null) {
            // no current version nothing to rollback
            logger.info("Rollback: nothing to rollback")
        } else {
            val versionCompare = currentVersion.versionCompare(migration.version)
            if (versionCompare < 0) {
                logger.info("Rollback: requested version ${migration.version} is greater than current version $currentVersion, use migrate instead")
                return
            } else if (versionCompare >= 0) {
                // need to run all down migrations from current version for all up migrations that were run
                val migrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, currentVersion).values.toList().sortedByDescending { it.entityResourcePath }
                val appliedMigrations = migration.getVersionBatchesNameMap()

                if (!migrations.isEmpty()) {
                    migrations.forEach { entityScript ->
                        val migrationScriptPath = DbEntity.MIGRATION.addSuffix(DbEntity.ROLLBACK.removeSuffix(entityScript.entityResourcePath))

                        if (appliedMigrations.containsKey(migrationScriptPath)) {
                            // apply the down migration
                            val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                            migration.insertDownMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                                logger.info("Rollback ${entityScript.entityResourcePath}")
                                runBatchScript(entity, migration, entityScript.entityResourcePath, null, sqlScript, entityScript)
                            }
                        }
                    }
                } else {
                    logger.debug("Rollback: no down migrations in current version $currentVersion")
                }

                if (versionCompare > 0) {
                    // need to run all migrations from earlier versions down up to but not including requested version
                    val versionList = getVersions().filter { it.compareTo(currentVersion) > 0 && it.compareTo(migration.version) < 0 }.sortedWith(Comparator(String::versionCompare)).reversed()

                    versionList.forEach { version ->
                        val versionMigrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, version).values.toList().sortedByDescending { it.entityResourcePath }

                        if (!versionMigrations.isEmpty()) {
                            versionMigrations.forEach { entityScript ->
                                // apply the migration
                                val migrationScriptPath = entityScript.entityResourcePath
                                val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                                migration.insertDownMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                                    logger.info("Rollback ${entityScript.entityResourcePath}")
                                    runBatchScript(entity, migration, migrationScriptPath, null, sqlScript, entityScript)
                                }
                            }
                        } else {
                            logger.debug("Rollback: no down migrations in version $version")
                        }
                    }
                }
            }

            if (versionCompare == 0) {
                val prevVersion = getPreviousVersion(migration.version)
                if (prevVersion != null) {
                    // create all tables from the version before the requested which do not exist, this will happen when a table is deleted in a later version
                    val prevMigration = migration.withVersion(prevVersion)

                    updateEntities(DbEntity.FUNCTION, prevMigration)
                    createTables(prevMigration)

                    // validate that current db tables and their definition matches the table list
                    val sb = StringBuilder()
                    validateTableResourceFiles(prevVersion, sb)
                    if (!sb.isEmpty()) {
                        // insert migration line
                        prevMigration.insertMigrationAfter("<validation failure>", sb.toString()) {}
                    }

                    updateEntities(DbEntity.VIEW, prevMigration)
                    updateEntities(DbEntity.TRIGGER, prevMigration)
                    updateEntities(DbEntity.PROCEDURE, prevMigration)

                    prevMigration.insertMigrationAfter("<rollback>", "") {}
                } else {
                    // no previous version to roll back to for table or proc info
                    logger.debug("Rollback: rolled back to start of history at pre-migrations for ${migration.version}")
                    migration.insertMigrationAfter("<rollback>", "# start of history") {}
                }
            } else {
                updateEntities(DbEntity.FUNCTION, migration)

                createTables(migration)

                // validate that current db tables and their definition matches the table list
                val sb = StringBuilder()
                validateTableResourceFiles(migration.version, sb)
                if (!sb.isEmpty()) {
                    // insert migration line
                    migration.insertMigrationAfter("<validation failure>", sb.toString()) {}
                }

                updateEntities(DbEntity.VIEW, migration)
                updateEntities(DbEntity.TRIGGER, migration)
                updateEntities(DbEntity.PROCEDURE, migration)

                migration.insertMigrationAfter("<rollback>", "") {}
            }
        }
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
     *     create-tables            - create all tables which do not exist
     *     validate-tables          - validate that version table scripts and database agree
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
     *
     *     exit                     - exit application
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

                        "dump-tables" -> {
                            if (dbVersion == null) dbVersion = getCurrentVersion()
                            dumpTables(dbPath, dbVersion!!)
                        }

                        "validate" -> {
                            if (dbVersion == null) dbVersion = getCurrentVersion()
                            validateTableResourceFiles(dbVersion!!)
                        }

                        "validate-tables" -> {
                            if (dbVersion == null) dbVersion = getCurrentVersion()
                            validateTables(dbPath, dbVersion!!)
                        }

                        "init" -> {
                            if (migration != null) {
                                throw IllegalArgumentException("db init command must be first executed command")
                            }
                            migration = initMigrations(dbVersion)
                        }

                        "migrate" -> {
                            // here need to apply up migrations from current version to given version or latest in version sorted order
                            if (migration == null) migration = initMigrations(dbVersion ?: getLatestVersion())

                            migrate(migration!!)
                        }

                        "rollback" -> {
                            // here need to apply down migrations from current version to given version or if none given then rollback the last batch which was not rolled back
                            if (migration == null) migration = initMigrations(dbVersion)

                            rollback(migration!!)
                        }

                        "create-tables", "create-tbls" -> {
                            if (migration == null) migration = initMigrations(dbVersion)
                            createTables(migration!!)
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

                        "exit" -> {
                            System.exit(1)
                        }

                        else -> {
                            throw IllegalArgumentException("db option $option is not recognized")
                        }
                    }
                }
            } catch (e: Exception) {
                val migrationSession = migration
                if (migrationSession != null) {
                    tx.rollback()

                    tx.begin()
                    val migrationSql = migrationSession.getMigrationSql(
                        migrationSession.lastScriptName ?: "",
                        migrationSession.lastScriptSql ?: ""
                    ).inParams("lastProblem" to e.message)

                    tx.execute(migrationSql)
                    tx.commit()
                }

                // re-throw exception
                throw e
            }

            tx.commit()
        }
    }
}

