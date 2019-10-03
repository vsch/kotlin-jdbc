package com.vladsch.kotlin.jdbc

import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileWriter
import java.nio.charset.Charset
import java.sql.SQLException
import kotlin.system.exitProcess

@Suppress("MemberVisibilityCanBePrivate")
class Migrations(val sessions: Map<String, Session>, val migrationSessions: Map<String, Session>, val dbEntityExtractor: DbEntityExtractor, val resourceClass: Class<*>) {
    constructor(session: Session, migrationSession: Session, dbEntityExtractor: DbEntityExtractor, resourceClass: Class<*>)
        : this(mapOf(DEFAULT_PROFILE to session), mapOf(DEFAULT_PROFILE to migrationSession), dbEntityExtractor, resourceClass)

    companion object {
        private val LOG = LoggerFactory.getLogger(Migrations::class.java)!!
        val MIGRATIONS_FILE_NAME = DbEntity.TABLE.addSuffix("migrations")
        val CLEAN_COMMENT = "(?<=^|\n)#".toRegex()
        val DEFAULT_PROFILE = "default"
        val MIGRATIONS_TABLE = "migrations"
    }

    var quiet = false
    var verbose = false
    var detailed = false
    var dbProfile: String? = null
    var migration: MigrationSession? = null

    fun getVersions(): List<String> {
        return getResourceFiles(resourceClass, "/db/$dbProfile").filter { it.matches(DbVersion.regex) }.map { it.toUpperCase() }.sortedWith(Comparator(String::versionCompare))
    }

    fun getDbProfiles(): List<String> {
        return sessions.keys.toList()
        //        val dbFiles = getResourceFiles(resourceClass, "/db")
        //        val dbProfiles = dbFiles.filter { it != "templates"}
        //        return dbProfiles.filter { sessions.containsKey(it) }
    }

    fun getPreviousVersion(version: String): String? {
        val versions = getVersions()
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
        return if (versions.isEmpty()) "V0_0_0" else versions.last()
    }

    val session: Session get() = sessions[dbProfile] ?: sessions[DEFAULT_PROFILE]!!
    val migrationSession: Session get() = migrationSessions[dbProfile] ?: sessions[DEFAULT_PROFILE]!!

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
        val tablesDir = entity.getEntityDirectory(dbDir, dbProfile!!, dbVersion, true)
        val tables = dbEntityExtractor.getDbEntities(entity, session)
        doForEachEntity(DbEntity.TABLE, tablesDir, tables, consumer)
    }

    fun forEachEntity(entity: DbEntity, dbVersion: String, consumer: (tableFile: File, tableScript: String) -> Unit) {
        val tablesDir = entity.getEntityResourceDirectory(dbProfile!!, dbVersion)
        val tables = dbEntityExtractor.getDbEntities(entity, session)
        doForEachEntity(DbEntity.TABLE, tablesDir, tables, consumer)
    }

    fun forEachEntityFile(dbDir: File, dbVersion: String, consumer: (tableFile: File) -> Unit, entity: DbEntity) {
        val tablesDir = entity.getEntityDirectory(dbDir, dbProfile!!, dbVersion, true)
        val tableFiles = entity.getEntityFiles(tablesDir)

        // delete all existing table sql files
        tableFiles.forEach {
            val file = File(it)
            consumer.invoke(file)
        }
    }

    fun forEachEntityResourceFile(entity: DbEntity, dbVersion: String, consumer: (tableFile: File) -> Unit) {
        val tablesDir = entity.getEntityResourceDirectory(dbProfile!!, dbVersion)
        val tableFiles = entity.getEntityResourceFiles(resourceClass, dbProfile!!, dbVersion)

        // delete all existing table sql files
        tableFiles.forEach {
            val file = tablesDir + it
            consumer.invoke(file)
        }
    }

    fun dumpTables(dbDir: File, dbVersion: String) {
        forEachEntityFile(dbDir, dbVersion, { tableFile ->
            tableFile.delete()
        }, DbEntity.TABLE)

        forEachEntity(DbEntity.TABLE, dbDir, dbVersion) { tableFile, tableScript ->
            if (tableFile.name != MIGRATIONS_FILE_NAME) {
                val tableWriter = FileWriter(tableFile)
                tableWriter.write(tableScript)
                tableWriter.flush()
                tableWriter.close()
            }
        }
    }

    fun doesResourceExist(resourcePath: String): Boolean {
        return getResourceAsStream(resourceClass, resourcePath) != null
    }

    fun equalWithOutOfOrderLines(text1: String, text2: String): Boolean {
        val textLines1 = text1.trim().split('\n').filter { !it.isBlank() }.map { it.trim().removeSuffix(",").trim() }
        val textLines2 = text2.trim().split('\n').filter { !it.isBlank() }.map { it.trim().removeSuffix(",").trim() }
        if (textLines1.size == textLines2.size) {
            val textLinesMap = HashMap<String, Int>()
            textLines2.forEach {
                textLinesMap[it] = (textLinesMap[it] ?: 0) + 1
            }

            for (line in textLines1) {
                if (!textLinesMap.containsKey(line)) return false
                val count = textLinesMap[line]!! - 1

                if (count <= 0) textLinesMap.remove(line)
                else textLinesMap[line] = count
            }

            if (textLinesMap.isEmpty()) return true
        }
        return false
    }

    fun logTableDetails(tableName: String, errorAppendable: Appendable?, resourceScript: String?, databaseScript: String?) {
        if (detailed) {
            val out = StringBuilder()

            if (resourceScript != null) {
                out.append("\n------------------------------------------ RESOURCE ------------------------------------------\n$resourceScript\n")
            }
            if (databaseScript != null) {
                out.append("\n------------------------------------------ DATABASE ------------------------------------------\n$databaseScript\n")
            }

            if (resourceScript != null || databaseScript != null) {
                out.append("----------------------------------------------------------------------------------------------\n")
            }

            LOG.error(out.toString())
            errorAppendable?.append(out)
        }
    }

    fun validateTableResourceFiles(dbVersion: String, errorAppendable: Appendable? = null): Boolean {
        val tableSet = HashSet<String>()
        var validationPassed = true
        val entity = DbEntity.TABLE

        // validate that all table resources are valid
        forEachEntityResourceFile(entity, dbVersion) { tableFile ->
            if (tableFile.name != MIGRATIONS_FILE_NAME) {
                val tableSql = getResourceAsString(resourceClass, tableFile.path)
                val tableName = entity.extractEntityName(dbEntityExtractor, tableSql)
                if (tableName == null) {
                    val s = "Invalid SQL ${entity.displayName} file ${tableFile.path}, cannot find ${entity.displayName} name"
                    LOG.error(s)
                    errorAppendable?.appendln(s)
                    logTableDetails(entity.displayName, errorAppendable, tableSql, null)
                    validationPassed = false
                } else if (tableName + entity.fileSuffix != tableFile.name) {
                    val s = "File ${tableFile.path} for ${entity.displayName} $tableName should be named $tableName${entity.fileSuffix}"
                    LOG.error(s)
                    errorAppendable?.appendln(s)
                    logTableDetails(entity.displayName, errorAppendable, tableSql, null)
                    validationPassed = false
                }
            }
        }

        forEachEntity(entity, dbVersion) { tableFile, tableScript ->
            if (tableFile.name != MIGRATIONS_FILE_NAME) {
                tableSet.add(tableFile.path)

                if (doesResourceExist(tableFile.path)) {
                    val tableSql = getResourceAsString(resourceClass, tableFile.path)
                    val tableName = entity.extractEntityName(dbEntityExtractor, tableSql)
                    if (tableName == null) {
                        val s = "Invalid SQL ${entity.displayName} file ${tableFile.path}, cannot find ${entity.displayName} name"
                        LOG.error(s)
                        errorAppendable?.appendln(s)
                        logTableDetails(entity.displayName, errorAppendable, tableSql, null)
                        validationPassed = false
                    } else if (tableName + entity.fileSuffix != tableFile.name) {
                        val s = "File ${tableFile.path} for ${entity.displayName} $tableName should be named $tableName${entity.fileSuffix}"
                        LOG.error(s)
                        errorAppendable?.appendln(s)
                        logTableDetails(entity.displayName, errorAppendable, tableSql, null)
                        validationPassed = false
                    } else {
                        if (tableSql.trim() != tableScript.trim()) {
                            // see if rearranging lines will make them equal
                            if (!equalWithOutOfOrderLines(tableSql, tableScript)) {
                                val s = "Table validation failed for ${tableFile.path}, database and resource differ"
                                if (validationPassed) {
                                    //                                    val tmp = 0
                                }
                                if (verbose) LOG.error(s)
                                if (errorAppendable != null) {
                                    if (!verbose) LOG.error(s)
                                    errorAppendable.appendln(s)
                                }
                                logTableDetails(entity.displayName, errorAppendable, tableSql, tableScript)
                                validationPassed = false
                            }
                        }
                    }
                } else {
                    val s = "Table validation failed for ${tableFile.path}, resource is missing"
                    if (verbose) LOG.error(s)
                    if (errorAppendable != null) {
                        if (!verbose) LOG.error(s)
                        errorAppendable.appendln(s)
                    }
                    logTableDetails(entity.displayName, errorAppendable, null, tableScript)
                    validationPassed = false
                }
            }
        }

        if (validationPassed) {
            forEachEntityResourceFile(DbEntity.TABLE, dbVersion) { tableFile ->
                if (tableFile.name != MIGRATIONS_FILE_NAME) {
                    if (!tableSet.contains(tableFile.path)) {
                        val s = "Table validation failed for ${tableFile.path}, no database table for resource"
                        if (verbose) LOG.error(s)
                        if (errorAppendable != null) {
                            if (!verbose) LOG.error(s)
                            errorAppendable.appendln(s)
                        }
                        validationPassed = false
                    }
                }
            }
        }

        return validationPassed
    }

    /**
     * Create all tables which exist in tables snapshot but not in the database
     *
     * Creating new tables does not use migrations and uses snapshot instead
     *
     * @param migration migration session
     */
    fun createTables(migration: MigrationSession) {
        val entity = DbEntity.TABLE

        // may need to create table directory
        val tables = dbEntityExtractor.getDbEntities(entity, session)
        val tableSet = tables.map { it.toLowerCase() }.toSet()
        val entities = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, migration.migrations.dbProfile!!, migration.version)

        runEntitiesSql(migration, entities, "Creating table", null) { tableSet.contains(it.toLowerCase()) }
    }

    private fun runEntitiesSql(migration: MigrationSession, entities: Map<String, DbEntity.EntityData>, opType: String, dropEntityIfExists: String? = null, excludeFilter: ((String) -> Boolean)? = null) {
        for ((entityName, entityEntry) in entities) {
            if (excludeFilter == null || !excludeFilter.invoke(entityName)) {
                val entityRealName = entityEntry.entityName
                val entityFile = entityEntry.entityResourcePath
                val entitySqlContents = entityEntry.entitySql

                val query = sqlQuery(entitySqlContents)
                LOG.info("$opType $entityRealName from $entityFile")
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
        val entities = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, migration.migrations.dbProfile!!, migration.version)
        runEntitiesSql(migration, entities, "Update ${entity.displayName}", if (entity == DbEntity.TABLE) null else entity.dbEntity, excludeFilter)

        // delete all which do not exist in files
        val dbEntities = dbEntityExtractor.getDbEntities(entity, session)
        dbEntities.forEach { dbEntityName ->
            if (!entities.containsKey(dbEntityName.toLowerCase())) {
                val dropEntitySql = dbEntityExtractor.getDropEntitySql(entity, dbEntityName)
                LOG.info("Dropping ${entity.displayName} $dbEntityName")
                session.execute(sqlQuery(dropEntitySql))
            }
        }
    }

    fun copyEntities(entity: DbEntity, sourceVersionDir: File, destinationVersionDir: File, deleteDestinationFiles: Boolean, excludeFilter: ((String) -> Boolean)? = null) {
        // may need to create table directory
        if (deleteDestinationFiles) {
            val destEntities = entity.getEntityFiles(destinationVersionDir + entity.dbEntityDirectory)
            destEntities.forEach {
                if (excludeFilter == null || !excludeFilter.invoke(it)) {
                    val fileName = File(it).name
                    val destinationFile = (destinationVersionDir + entity.dbEntityDirectory) + fileName
                    destinationFile.delete()
                }
            }
        }

        val entities = entity.getEntityFiles(sourceVersionDir + entity.dbEntityDirectory)
        entities.forEach {
            if (excludeFilter == null || !excludeFilter.invoke(it)) {
                val fileName = File(it).name
                val sourceFile = (sourceVersionDir + entity.dbEntityDirectory) + fileName
                val destinationFile = (destinationVersionDir + entity.dbEntityDirectory) + fileName
                sourceFile.copyTo(destinationFile)
            }
        }
    }

    /**
     * Take the latest actual fully applied migration or rollback version, failing that take version with latest steps
     */
    fun getCurrentVersion(): String? {
        return migrationSession.first(sqlQuery("""
SELECT version FROM migrations
WHERE rolled_back_id IS NULL AND last_problem IS NULL AND script_name IN ('<migrate>', '<rollback>')
ORDER BY migration_id DESC
LIMIT 1
""")) { row ->
            row.string(1)
        } ?: migrationSession.first(sqlQuery("""
SELECT version FROM migrations
WHERE rolled_back_id IS NULL AND last_problem IS NULL AND migration_type IS NOT NULL
ORDER BY migration_id DESC
LIMIT 1
""")) { row ->
            row.string(1)
        }
    }

    fun initMigrations(dbVersion: String? = null): MigrationSession {
        val entity = DbEntity.TABLE

        var migration: MigrationSession
        val entityFixer = dbEntityExtractor.entityScriptFixer(DbEntity.TABLE, session)
        val entitySql = sqlQuery(dbEntityExtractor.getShowEntitySql(DbEntity.TABLE, MIGRATIONS_TABLE))
        val entityCreate = try {
            session.first(entitySql) {
                it.string(2)
            }
        } catch (e: Exception) {
            null
        }

        if (entityCreate == null) {
            var latestMatchedVersion: String? = null

            if (dbVersion == null) {
                val versionList = getVersions()
                    .sortedWith(Comparator(String::versionCompare))

                versionList.forEach { it ->
                    if (validateTableResourceFiles(it, null)) {
                        latestMatchedVersion = it
                    }
                }

                if (latestMatchedVersion != null) {
                    LOG.info("Matched version $latestMatchedVersion based on table schema")
                } else {
                    LOG.info("No version matched based on table schema, setting version to V0_0_0")
                }
            }

            val useDbVersion = dbVersion ?: "V0_0_0"

            val dbTableResourceDir = entity.getEntityResourceDirectory(dbProfile!!, useDbVersion)
            val tableEntities = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, dbProfile!!, useDbVersion)

            // create the table
            val scriptName: String
            val scriptSql: String
            migration = MigrationSession(1, useDbVersion, this)

            if (tableEntities.contains(MIGRATIONS_TABLE)) {
                // run the file found for the version
                val tableEntry = tableEntities[MIGRATIONS_TABLE]!!

                LOG.info("Creating migrations table from ${dbTableResourceDir.path}/${tableEntry.entityResourcePath}")
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

            // switch to latest if none given
            migration = migration.withVersion(latestMatchedVersion ?: dbVersion ?: getLatestVersion())

            if (latestMatchedVersion != null || dbVersion != null) {
                // insert <migrated> line so the rest know what version it is
                migration.insertUpMigrationAfter("<migrate>", "") {}
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
        val entity = DbEntity.MIGRATION
        val currentVersion = getCurrentVersion() ?: "V0_0_0"

        val versionCompare = currentVersion.versionCompare(migration.version)
        if (versionCompare > 0) {
            LOG.info("Migrate: requested version ${migration.version} is less than current version $currentVersion, use rollback instead")
            return
        } else if (versionCompare <= 0) {
            // need to run all up migrations from current version which have not been run
            val migrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, dbProfile!!, currentVersion).values.toList().sortedWith(DbEntity.MIGRATIONS_COMPARATOR)

            if (!migrations.isEmpty()) {
                val appliedMigrations = migration.getVersionBatchesNameMap()

                migrations.forEach { entityScript ->
                    val migrationScriptPath = entityScript.entityResourcePath
                    val currentMigration = migration.withVersion(currentVersion)

                    if (!appliedMigrations.containsKey(entityScript.entityResourcePath)) {
                        // apply the migration
                        val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                        currentMigration.insertUpMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                            LOG.info("Migrate ${entityScript.entityResourcePath}")
                            runBatchScript(entity, currentMigration, migrationScriptPath, appliedMigrations, sqlScript, entityScript)
                        }
                    }
                }
            } else {
                LOG.debug("Migrate: no migrations in current version $currentVersion")
            }

            if (versionCompare < 0) {
                // need to run all migrations from later versions up to requested version
                val versionList = getVersions()
                    .filter { it.versionCompare(currentVersion) > 0 && (it.versionCompare(migration.version) <= 0) }
                    .sortedWith(Comparator(String::versionCompare))

                versionList.forEach { version ->
                    val versionMigrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, dbProfile!!, version)
                        .values.toList()
                        .sortedWith(DbEntity.MIGRATIONS_COMPARATOR)

                    val versionMigration = migration.withVersion(version)

                    if (!versionMigrations.isEmpty()) {
                        versionMigrations.forEach { entityScript ->
                            // apply the migration
                            val appliedMigrations = versionMigration.getVersionBatchesNameMap()
                            val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                            versionMigration.insertUpMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                                LOG.info("Migrate ${entityScript.entityResourcePath}")
                                runBatchScript(entity, versionMigration, entityScript.entityResourcePath, appliedMigrations, sqlScript, entityScript)
                            }
                        }
                    } else {
                        LOG.debug("Migrate: no up migrations in version $version")
                    }
                }
            }
        }

        // run all updates from requested version
        updateEntities(DbEntity.FUNCTION, migration)

        // validate that current db tables and their definition matches the table list
        val sb = StringBuilder()
        validateTableResourceFiles(migration.version, sb)
        if (!sb.isEmpty()) {
            // insert migration line
            migration.insertMigrationAfter("<table validation failure>", sb.toString()) {}
        }

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
        entityData: DbEntity.EntityData
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
                val migrationPartName = "$migrationScriptPath[$index:$startLine-${line - 1}]"
                val query = sqlQuery(sql)
                if (opType == DbEntity.MIGRATION) {
                    if (appliedMigrations == null || !appliedMigrations.containsKey(migrationPartName)) {
                        migration.insertUpMigrationAfter(migrationPartName, sql) {
                            LOG.info("Migrate ${entityData.entityResourcePath} part [$index:$startLine-${line - 1}]")
                            try {
                                session.execute(query)
                            } catch (e: SQLException) {
                                LOG.error("SQLException: ${e.message} on SQL:\n$sql\n")
                                throw e
                            }
                        }
                    }
                } else {
                    if (appliedMigrations == null || appliedMigrations.containsKey(migrationPartName)) {
                        migration.insertDownMigrationAfter(migrationPartName, sql) {
                            LOG.info("Rollback ${entityData.entityResourcePath} part [$index:$startLine-${line - 1}]")
                            session.execute(query)
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
        val entity = DbEntity.ROLLBACK
        val currentVersion = getCurrentVersion()
        if (currentVersion == null) {
            // no current version nothing to rollback
            LOG.info("Rollback: nothing to rollback")
        } else {
            val versionCompare = currentVersion.versionCompare(migration.version)
            if (versionCompare < 0) {
                LOG.info("Rollback: requested version ${migration.version} is greater than current version $currentVersion, use migrate instead")
                return
            } else if (versionCompare >= 0) {
                // need to run all down migrations from current version for all up migrations that were run
                val migrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, dbProfile!!, currentVersion)
                    .values.toList()
                    .sortedWith(DbEntity.MIGRATIONS_COMPARATOR)
                    .reversed()

                val appliedMigrations = migration.getVersionBatchesNameMap()

                if (!migrations.isEmpty()) {
                    migrations.forEach { entityScript ->
                        val migrationScriptPath = DbEntity.MIGRATION.addSuffix(DbEntity.ROLLBACK.removeSuffix(entityScript.entityResourcePath))
                        val currentMigration = migration.withVersion(currentVersion)

                        if (appliedMigrations.containsKey(migrationScriptPath)) {
                            // apply the down migration
                            val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                            currentMigration.insertDownMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                                LOG.info("Rollback ${entityScript.entityResourcePath}")
                                runBatchScript(entity, currentMigration, entityScript.entityResourcePath, null, sqlScript, entityScript)
                            }
                        }
                    }
                } else {
                    LOG.debug("Rollback: no down migrations in current version $currentVersion")
                }

                if (versionCompare > 0) {
                    // need to run all migrations from earlier versions down up to but not including requested version
                    val versionList = getVersions()
                        .filter { it.versionCompare(currentVersion) < 0 && it.versionCompare(migration.version) > 0 }
                        .sortedWith(Comparator(String::versionCompare))
                        .reversed()

                    versionList.forEach { version ->
                        val versionMigrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, dbProfile!!, version)
                            .values.toList()
                            .sortedWith(DbEntity.MIGRATIONS_COMPARATOR)
                            .reversed()

                        val versionMigration = migration.withVersion(version)

                        if (!versionMigrations.isEmpty()) {
                            versionMigrations.forEach { entityScript ->
                                // apply the migration
                                val migrationScriptPath = entityScript.entityResourcePath
                                val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                                versionMigration.insertDownMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                                    LOG.info("Rollback ${entityScript.entityResourcePath}")
                                    runBatchScript(entity, versionMigration, migrationScriptPath, null, sqlScript, entityScript)
                                }
                            }
                        } else {
                            LOG.debug("Rollback: no down migrations in version $version")
                        }
                    }
                }
            }

            if (versionCompare == 0) {
                val prevVersion = getPreviousVersion(migration.version)
                if (prevVersion != null) {
                    val prevMigration = migration.withVersion(prevVersion)

                    updateEntities(DbEntity.FUNCTION, prevMigration)

                    // validate that current db tables and their definition matches the table list
                    val sb = StringBuilder()
                    validateTableResourceFiles(prevVersion, sb)
                    if (!sb.isEmpty()) {
                        // insert migration line
                        prevMigration.insertMigrationAfter("<table validation failure>", sb.toString()) {}
                    }

                    updateEntities(DbEntity.VIEW, prevMigration)
                    updateEntities(DbEntity.TRIGGER, prevMigration)
                    updateEntities(DbEntity.PROCEDURE, prevMigration)

                    prevMigration.insertMigrationAfter("<rollback>", "") {}
                } else {
                    // no previous version to roll back to for table or proc info
                    LOG.debug("Rollback: rolled back to start of history at pre-migrations for ${migration.version}")
                    migration.insertMigrationAfter("<rollback>", "# start of history") {}
                }
            } else {
                updateEntities(DbEntity.FUNCTION, migration)

                // validate that current db tables and their definition matches the table list
                val sb = StringBuilder()
                validateTableResourceFiles(migration.version, sb)
                if (!sb.isEmpty()) {
                    // insert migration line
                    migration.insertMigrationAfter("<table validation failure>", sb.toString()) {}
                }

                updateEntities(DbEntity.VIEW, migration)
                updateEntities(DbEntity.TRIGGER, migration)
                updateEntities(DbEntity.PROCEDURE, migration)

                migration.insertMigrationAfter("<rollback>", "") {}
            }
        }
    }

    fun updateSchema(dbDir: File, dbVersion: String) {
        dbDir.ensureExistingDirectory("dbDir")
        val versionDir = getVersionDirectory(dbDir, dbProfile!!, dbVersion, false)

        val dbProfileDir = dbDir + dbProfile!!
        dbProfileDir.ensureExistingDirectory("db/$dbProfile")

        val schemaDir = dbProfileDir + "schema"
        schemaDir.ensureCreateDirectory("db/$dbProfile/schema")

        val versionFile = schemaDir + "version.txt"
        versionFile.writeText("""# Version: ${dbVersion}""".trimIndent())

        val functionsDir = schemaDir + DbEntity.FUNCTION.dbEntityDirectory
        //val migrationsDir = schemaDir + DbEntity.MIGRATION.dbEntityDirectory
        val proceduresDir = schemaDir + DbEntity.PROCEDURE.dbEntityDirectory
        val tablesDir = schemaDir + DbEntity.TABLE.dbEntityDirectory
        val triggerDir = schemaDir + DbEntity.TRIGGER.dbEntityDirectory
        val viewsDir = schemaDir + DbEntity.VIEW.dbEntityDirectory

        functionsDir.ensureCreateDirectory("db/$dbProfile/schema/" + DbEntity.FUNCTION.dbEntityDirectory)
        // migrationsDir.ensureCreateDirectory("db/$dbProfile/schema/" + DbEntity.MIGRATION.dbEntityDirectory)
        proceduresDir.ensureCreateDirectory("db/$dbProfile/schema/" + DbEntity.PROCEDURE.dbEntityDirectory)
        tablesDir.ensureCreateDirectory("db/$dbProfile/schema/" + DbEntity.TABLE.dbEntityDirectory)
        triggerDir.ensureCreateDirectory("db/$dbProfile/schema/" + DbEntity.TRIGGER.dbEntityDirectory)
        viewsDir.ensureCreateDirectory("db/$dbProfile/schema/" + DbEntity.VIEW.dbEntityDirectory)

        // copy all entities from given version except migrations to snapshot dir
        copyEntities(DbEntity.FUNCTION, versionDir, schemaDir, true)
        //copyEntities(DbEntity.MIGRATION, versionDir, snapshotDir)
        //copyEntities(DbEntity.ROLLBACK, versionDir, snapshotDir)
        copyEntities(DbEntity.PROCEDURE, versionDir, schemaDir, true)
        copyEntities(DbEntity.TABLE, versionDir, schemaDir, true) { it.toLowerCase() == MIGRATIONS_FILE_NAME }
        copyEntities(DbEntity.TRIGGER, versionDir, schemaDir, true)
        copyEntities(DbEntity.VIEW, versionDir, schemaDir, true)
    }

    fun newVersion(dbDir: File, dbVersion: String) {
        val versionDir = getVersionDirectory(dbDir, dbProfile!!, dbVersion, null)

        if (versionDir.exists()) {
            throw IllegalArgumentException("Version directory '${versionDir.path}' must not exist")
        }

        if (!versionDir.mkdirs()) {
            throw IllegalStateException("Version directory '${versionDir.path}' could not be created")
        }

        val functionsDir = versionDir + DbEntity.FUNCTION.dbEntityDirectory
        val migrationsDir = versionDir + DbEntity.MIGRATION.dbEntityDirectory
        val proceduresDir = versionDir + DbEntity.PROCEDURE.dbEntityDirectory
        val tablesDir = versionDir + DbEntity.TABLE.dbEntityDirectory
        val triggerDir = versionDir + DbEntity.TRIGGER.dbEntityDirectory
        val viewsDir = versionDir + DbEntity.VIEW.dbEntityDirectory

        functionsDir.ensureCreateDirectory("db/$dbProfile/$dbVersion/" + DbEntity.FUNCTION.dbEntityDirectory)
        migrationsDir.ensureCreateDirectory("db/$dbProfile/$dbVersion/" + DbEntity.MIGRATION.dbEntityDirectory)
        proceduresDir.ensureCreateDirectory("db/$dbProfile/$dbVersion/" + DbEntity.PROCEDURE.dbEntityDirectory)
        tablesDir.ensureCreateDirectory("db/$dbProfile/$dbVersion/" + DbEntity.TABLE.dbEntityDirectory)
        triggerDir.ensureCreateDirectory("db/$dbProfile/$dbVersion/" + DbEntity.TRIGGER.dbEntityDirectory)
        viewsDir.ensureCreateDirectory("db/$dbProfile/$dbVersion/" + DbEntity.VIEW.dbEntityDirectory)

        // copy all entities from previous version except migrations
        val previousVersion = getPreviousVersion(dbVersion)
        if (previousVersion != null) {
            copyEntities(DbEntity.FUNCTION, getVersionDirectory(dbDir, dbProfile!!, previousVersion, true), versionDir, true)
            //            copyEntities(DbEntity.MIGRATION, getVersionDirectory(dbDir, dbProfile!!, previousVersion, true), versionDir)
            //            copyEntities(DbEntity.ROLLBACK, getVersionDirectory(dbDir, dbProfile!!, previousVersion, true), versionDir)
            copyEntities(DbEntity.PROCEDURE, getVersionDirectory(dbDir, dbProfile!!, previousVersion, true), versionDir, true)
            copyEntities(DbEntity.TABLE, getVersionDirectory(dbDir, dbProfile!!, previousVersion, true), versionDir, true) { it.toLowerCase() == MIGRATIONS_FILE_NAME }
            copyEntities(DbEntity.TRIGGER, getVersionDirectory(dbDir, dbProfile!!, previousVersion, true), versionDir, true)
            copyEntities(DbEntity.VIEW, getVersionDirectory(dbDir, dbProfile!!, previousVersion, true), versionDir, true)

            // copy additional files from the templates directory
            copyExtraTemplateFiles(dbDir, dbVersion)
        }
    }

    fun copyExtraTemplateFiles(dbDir: File, dbVersion: String) {
        val versionDir = getVersionDirectory(dbDir, dbProfile!!, dbVersion, false)
        val extraResourceFiles = getExtraSampleFiles(resourceClass)

        extraResourceFiles.forEach {
            val resourcePath = File(it)
            val resourceName = resourcePath.name

            if (!DbEntity.isEntityDirectory(resourceName)) {
                val entityFile = versionDir + resourceName

                val entitySample = getResourceAsString(resourceClass, it)
                entityFile.writeText(entitySample.replace("__VERSION__".toRegex(), dbVersion.replace('_', '.')))
            }
        }
    }

    fun newEntityFile(entity: DbEntity, dbDir: File, dbVersion: String, entityName: String): Pair<File, File> {
        val versionDir = getVersionDirectory(dbDir, dbProfile!!, dbVersion, false)

        val entityDir = versionDir + entity.dbEntityDirectory

        entityDir.ensureCreateDirectory("db/$dbProfile/$dbVersion/${entity.dbEntityDirectory}")

        if (entity == DbEntity.MIGRATION || entity == DbEntity.ROLLBACK) {
            var lastMigration = 0

            entity.getEntityFiles(entityDir).forEach {
                val (num, _) = File(it).name.extractLeadingDigits()
                if (num != null && num > lastMigration) {
                    lastMigration = num
                }
            }

            entity.getEntityFiles(entityDir).forEach {
                val (num, _) = File(it).name.extractLeadingDigits()
                if (num != null && num > lastMigration) {
                    lastMigration = num
                }
            }

            lastMigration++

            val migrationFile = entityDir + "$lastMigration.$entityName${DbEntity.MIGRATION.fileSuffix}"
            val rollbackFile = entityDir + "$lastMigration.$entityName${DbEntity.ROLLBACK.fileSuffix}"

            val migrationSample = DbEntity.MIGRATION.getEntitySample(dbDir, resourceClass)
            val rollbackSample = DbEntity.ROLLBACK.getEntitySample(dbDir, resourceClass)

            migrationFile.writeText(migrationSample.replace("__VERSION__".toRegex(), dbVersion).replace("__TITLE__".toRegex(), entityName))
            rollbackFile.writeText(rollbackSample.replace("__VERSION__".toRegex(), dbVersion).replace("__TITLE__".toRegex(), entityName))

            return Pair(migrationFile, rollbackFile)
        } else {
            val entityFile = entityDir + "$entityName${entity.fileSuffix}"
            val entitySample = entity.getEntitySample(dbDir, resourceClass)
            entityFile.writeText(entitySample.replace("__VERSION__".toRegex(), dbVersion).replace("__NAME__".toRegex(), entityName))

            return Pair(entityFile, entityFile)
        }
    }

    fun newEvolution(evolutionsDir: File, dbProfile: String, dbVersion: String) {
        evolutionsDir.ensureExistingDirectory("evolutions path")

        val evolutionsProfileDir = evolutionsDir + dbProfile
        evolutionsDir.ensureExistingDirectory("evolutions path/dbProfile")

        LOG.info("Generated new play evolution in ${evolutionsProfileDir.path}")

        val sb = StringBuilder()

        val versionMigrations = DbEntity.MIGRATION.getEntityResourceScripts(resourceClass, dbEntityExtractor, dbProfile!!, dbVersion)
            .values

        val versionRollbacks = DbEntity.ROLLBACK.getEntityResourceScripts(resourceClass, dbEntityExtractor, dbProfile!!, dbVersion)
            .values

        val migrationsMap = versionMigrations.map { it -> it.entityResourceName.replace("up.sql$".toRegex(), "") to it }.toMap()
        val rollbacksMap = versionRollbacks.map { it -> it.entityResourceName.replace("down.sql$".toRegex(), "") to it }.toMap()

        val entityNames = HashSet<String>()
        entityNames.addAll(migrationsMap.keys)
        entityNames.addAll(rollbacksMap.keys)

        val entityNameList = entityNames.toList()
            .sortedWith(DbEntity.MIGRATIONS_NAME_COMPARATOR)

        entityNameList.forEach { entityName ->
            migrationsMap[entityName]?.let { it ->
                appendEntityScript(sb, it, "# --- !Ups")
            }
            rollbacksMap[entityName]?.let { it ->
                appendEntityScript(sb, it, "# --- !Downs")
            }
        }

        if (!sb.isEmpty()) {
            var lastMigration = 0

            evolutionsProfileDir.list { _, name ->
                val (num, ext) = name.extractLeadingDigits()
                if (num != null && ext == ".sql" && num > lastMigration) {
                    lastMigration = num
                }
                false
            }

            lastMigration++

            val evolutionFile = evolutionsProfileDir + "$lastMigration.sql"
            evolutionFile.writeText(sb.toString())
            LOG.info("Generated new play evolution $lastMigration.sql in ${evolutionsDir.path}")
        } else {
            LOG.info("No migrations in $dbVersion for generating play evolution")
        }
    }

    private fun appendEntityScript(sb: StringBuilder, it: DbEntity.EntityData, commentPrefix: String): StringBuilder {
        sb.appendln(commentPrefix)
        val sqlScript = getResourceAsString(resourceClass, it.entityResourcePath)
        LOG.info("Adding to !Ups ${it.entityResourcePath}")
        sb.append("-- ").appendln(it.entityResourcePath)
        return sb.appendln(sqlScript.replace(CLEAN_COMMENT, "-- "))
    }

    fun importEvolutions(evolutionsDir: File, dbVersion: String, minEvolution: Int, maxEvolution: Int?, dbPath: File) {
        evolutionsDir.ensureExistingDirectory("evolutions path")

        LOG.info("Import play evolutions in ${evolutionsDir.path}, from [$minEvolution, ${maxEvolution ?: ""}]")

        val files = ArrayList<File>()
        val useMaxEvolution = maxEvolution ?: Int.MAX_VALUE
        evolutionsDir.listFiles().forEach {
            if (it.isFile && it.canRead()) {
                val evolutionNumber = it.nameWithoutExtension.toIntOrNull()
                if (evolutionNumber != null) {
                    if (evolutionNumber in minEvolution .. useMaxEvolution) {
                        LOG.info("Adding evolution file $it")
                        files.add(it)
                    }
                }
            }
        }

        files.sortBy { it.nameWithoutExtension.toInt() }

        files.forEach { file ->
            val lines = getFileContent(file).split("\n")

            // find the "# --- !Ups" and "# --- !Downs"
            val commonPrefix = StringBuilder()
            val ups = StringBuilder()
            val downs = StringBuilder()

            var sawUps = false
            var sawDowns = false
            var part = 1

            lines.forEach { line ->
                // SQL comment
                // make the Ups/Down flexible to handle user input
                val UPS = "# -{2,3} !Ups".toRegex()
                val DOWNS = "# -{2,3} !Downs".toRegex()
                val trimmed = line.trim()
                when {
                    trimmed.matches(UPS) -> {
                        if (sawDowns || sawUps) {
                            // one part is done
                            generateMigration(dbPath, dbVersion, file, part, ups, downs, true)
                            sawUps = false
                            sawDowns = false
                            part++
                        }

                        // copy common prefix to both
                        ups.append(commonPrefix)
                        downs.append(commonPrefix)
                        commonPrefix.clear()

                        sawUps = true
                        sawDowns = false
                        ups.append(line).append("\n")
                    }

                    trimmed.matches(DOWNS) -> {
                        downs.append(commonPrefix)
                        commonPrefix.clear()
                        sawDowns = true
                        downs.append(line).append("\n")
                    }

                    else -> {
                        if (!sawUps && !sawDowns) {
                            commonPrefix.append(line).append("\n")
                        } else if (sawUps && !sawDowns) {
                            ups.append(line).append("\n")
                        } else {
                            if (trimmed.isEmpty() || trimmed.startsWith("#") || trimmed.startsWith("-- ")) {
                                commonPrefix.append(line).append("\n")
                            } else {
                                downs.append(commonPrefix)
                                commonPrefix.clear()
                                downs.append(line).append("\n")
                            }
                        }
                    }
                }
            }

            generateMigration(dbPath, dbVersion, file, part, ups, downs, false)
        }
    }

    private fun generateMigration(dbPath: File, dbVersion: String, file: File, part: Int, ups: StringBuilder, downs: StringBuilder, moreParts: Boolean) {
        val partText = if (part > 1 || moreParts) ".$part" else ""
        val (migrationFile, rollbackFile) = newEntityFile(DbEntity.MIGRATION, dbPath, dbVersion, "evolution.${file.nameWithoutExtension}$partText")
        migrationFile.appendText(ups.toString(), Charset.forName("UTF-8"))
        rollbackFile.appendText(downs.toString(), Charset.forName("UTF-8"))
        LOG.info("Generated migration ${migrationFile.name} and rollback ${rollbackFile.name} for evolution ${file.nameWithoutExtension}$partText")
        ups.clear()
        downs.clear()
    }

    /**
     * Execute db command
     *
     * @param args Array<String>
     *
     *     flags
     *
     *          -v                  - verbose: show each comparison failure message in log
     *          -d                  - detail:
     *                                for validate-tables on failure log both scripts
     *          -q                  - quiet: sets quiet mode (turns off some default logging)
     *
     *     init                     - initialize migrations table and migrate all to given version or latest version
     *
     *     path "resources/db"      - path to resources/db directory
     *
     *     version "versionID"      - migrate to latest version and compare to snapshots
     *
     *                                "versionID" must be of the regex form V\d+(_\d+(_\d+(_.*)?)?)?"
     *
     *                                where the \d+ are major, minor, patch versions with the trailing .* being the version metadata.
     *                                Versions are compared using numeric comparison for major, minor and patch.
     *
     *                                The metadata if present will be compared using regular string comparison, ie. normal sort.
     *
     *     new-major                - create a new version directory with major version incremented.
     *     new-minor                - create a new version directory with minor version incremented.
     *     new-patch                - create a new version directory with patch version incremented.
     *
     *     new-evolution "play/evolutions/directory/path"         - create a new evolution from requested versions migrations and add this to the given play evolutions path
     *
     *     new-version              - create a new version directory for the requested version.
     *                                The directory cannot already exist. If the version is not provided
     *                                then the current version with its patch version number incremented will be used.
     *
     *                                All entity directories will be created, including migrations.
     *
     *                                If there is a previous version to the one requested then its all its entity scripts will be copied
     *                                to the new version directory.
     *
     *     new-migration "title"    - create a new up/down migration script files in the requested (or current) version's migrations
     *                                directory. The file name will be in the form: N.title.D.sql where N is numeric integer 1...,
     *                                D is up or down and title is the title passed command.
     *
     *     new-function "name"      - create a new function file using resources/db/templates customized template or built-in if none
     *     new-procedure "name"     - create a new procedure file using resources/db/templates customized template or built-in if none
     *     new-trigger "name"       - create a new trigger file using resources/db/templates customized template or built-in if none
     *     new-view "name"          - create a new view file using resources/db/templates customized template or built-in if none
     *
     *     migrate                  - migrate to given version or to latest version
     *
     *     rollback                 - rollback to given version or to previous version
     *
     *     dump-tables              - dump database tables
     *
     *     create-tables            - create all tables which exist in the version tables directory and which do not exist in the database
     *
     *     validate-tables          - validate that version table scripts and database agree
     *
     *     update-all               - update all: functions, views, procedures, triggers. This runs the scripts corresponding to
     *                                the database object.
     *
     *     update-procedures
     *     update-procs             - update stored procedures
     *
     *     update-functions
     *     update-funcs             - update functions
     *
     *     update-schema            - update db/schema directory with entities from selected version (or current if none given)
     *
     *     update-triggers          - update triggers
     *
     *     update-views             - update views
     *
     *     exit                     - exit application
     */
    fun dbCommand(args: Array<String>) {
        var dbVersion: String? = null
        var dbPath = File(System.getProperty("user.dir"))
        dbProfile = null

        var i = 0
        while (i < args.size) {
            val option = args[i++]

            when (option) {
                "-v" -> verbose = true
                "-d" -> detailed = true
                "-q" -> quiet = true

                "profile" -> {
                    if (args.size <= i) {
                        throw IllegalArgumentException("profile option requires a profile argument")
                    }
                    if (dbProfile != null) {
                        throw IllegalArgumentException("db profile command must come before commands that require profile")
                    }

                    val profile = args[i++]
                    if (!sessions.containsKey(profile)) {
                        throw IllegalArgumentException("profile $profile is not defined in sessions passed to Migrations.doCommand()")
                    }
                    //  val versions = getVersions()
                    //  if (!versions.contains(version.toUpperCase())) {
                    //      throw IllegalArgumentException("version $version does not exist in classpath '/db'")
                    //  }
                    dbProfile = profile
                }

                "version" -> {
                    if (args.size <= i) {
                        throw IllegalArgumentException("version option requires a version argument")
                    }

                    if (dbVersion != null) {
                        throw IllegalArgumentException("db version command must come before commands that require version")
                    }
                    val version = args[i++]
                    //  val versions = getVersions()
                    //  if (!versions.contains(version.toUpperCase())) {
                    //      throw IllegalArgumentException("version $version does not exist in classpath '/db'")
                    //  }
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

                "new-evolution" -> {
                    if (args.size < i) {
                        throw IllegalArgumentException("new-evolution option requires a path argument")
                    }
                    val path = args[i++]
                    val pathDir = File(path).ensureExistingDirectory("evolutions path")

                    execute(dbVersion) {
                        if (dbVersion == null) dbVersion = getCurrentVersion()

                        if (dbVersion != null) {
                            newEvolution(pathDir, dbProfile!!, dbVersion!!)
                        } else {
                            throw IllegalArgumentException("new-evolution option requires a database which has a current migration version")
                        }
                    }
                }

                "import-evolutions" -> {
                    if (args.size < i) {
                        throw IllegalArgumentException("import-evolution option requires a path argument")
                    }

                    val path = args[i++]

                    if (args.size < i) {
                        throw IllegalArgumentException("import-evolution option requires a min evolution argument")
                    }

                    val minEvoText = args[i++]
                    val maxEvoText: String? =
                        if (args.size < i) {
                            null
                        } else {
                            args[i++]
                        }

                    val pathDir = File(path).ensureExistingDirectory("evolutions")

                    if (dbProfile == null) dbProfile = DEFAULT_PROFILE
                    if (dbVersion == null) dbVersion = getCurrentVersion() ?: "V0_0_0"

                    val evolutionsProfileDir = (pathDir + dbProfile!!).ensureExistingDirectory("evolutions/dbProfile")

                    val minEvolution: Int = minEvoText.toIntOrNull()
                        ?: throw IllegalArgumentException("MinEvolution argument must be an integer")

                    val maxEvolution: Int? = if (maxEvoText == null) null else {
                        val maxEvo = maxEvoText.toIntOrNull()
                        if (maxEvo == null) i--
                        maxEvo
                    }

                    if (dbVersion != null) {
                        importEvolutions(evolutionsProfileDir, dbVersion!!, minEvolution, maxEvolution, dbPath)
                    } else {
                        throw IllegalArgumentException("import-evolution option requires a database which has a current migration version for selected profile")
                    }
                }

                "dump-tables" -> {
                    execute(dbVersion) {
                        val useDbVersion = dbVersion ?: getCurrentVersion()
                        if (!it) dbVersion = useDbVersion
                        dumpTables(dbPath, useDbVersion!!)
                    }
                }

                "update-schema" -> {
                    execute(dbVersion) {
                        val useDbVersion = dbVersion ?: getCurrentVersion()
                        if (!it) dbVersion = useDbVersion
                        updateSchema(dbPath, useDbVersion!!)
                    }
                }

                "validate-tables", "validate" -> {
                    execute(dbVersion) {
                        val useDbVersion = dbVersion ?: getCurrentVersion()
                        if (!it) dbVersion = useDbVersion
                        validateTableResourceFiles(useDbVersion!!)
                    }
                }

                "init" -> {
                    if (migration != null) {
                        throw IllegalArgumentException("db init command must be first executed command")
                    }

                    execute(dbVersion) {
                        val migrationInstance = initMigrations(dbVersion)
                        if (!it) migration = migrationInstance
                    }
                }

                "migrate" -> {
                    // here need to apply up migrations from current version to given version or latest in version sorted order
                    execute(dbVersion) {
                        val useDbVersion = dbVersion ?: getLatestVersion()
                        if (!it) dbVersion = useDbVersion

                        val migrationInstance = migration ?: initMigrations(useDbVersion)
                        if (!it) migration = migrationInstance
                        migrate(migrationInstance)
                    }
                }

                "rollback" -> {
                    // here need to apply down migrations from current version to given version or if none given then rollback the last batch which was not rolled back
                    executeProfile(option, dbVersion) {
                        migration = migration ?: initMigrations(dbVersion ?: getPreviousVersion(getCurrentVersion() ?: "V0_0_0"))
                        rollback(migration!!)
                    }
                }

                "create-tables", "create-tbls" -> {
                    execute(dbVersion) {
                        val migrationInstance = migration ?: initMigrations(dbVersion)
                        if (!it) migration = migrationInstance
                        createTables(migrationInstance)
                    }
                }

                "update-all" -> {
                    execute(dbVersion) {
                        val migrationInstance = migration ?: initMigrations(dbVersion)
                        if (!it) migration = migrationInstance

                        updateEntities(DbEntity.FUNCTION, migrationInstance)
                        updateEntities(DbEntity.VIEW, migrationInstance)
                        updateEntities(DbEntity.TRIGGER, migrationInstance)
                        updateEntities(DbEntity.PROCEDURE, migrationInstance)
                    }
                }

                "update-procedures", "update-procs" -> {
                    execute(dbVersion) {
                        val migrationInstance = migration ?: initMigrations(dbVersion)
                        if (!it) migration = migrationInstance
                        updateEntities(DbEntity.PROCEDURE, migrationInstance)
                    }
                }

                "update-functions", "update-funcs" -> {
                    execute(dbVersion) {
                        val migrationInstance = migration ?: initMigrations(dbVersion)
                        if (!it) migration = migrationInstance
                        updateEntities(DbEntity.FUNCTION, migrationInstance)
                    }
                }

                "update-views" -> {
                    execute(dbVersion) {
                        val migrationInstance = migration ?: initMigrations(dbVersion)
                        if (!it) migration = migrationInstance
                        updateEntities(DbEntity.VIEW, migrationInstance)
                    }
                }

                "update-triggers" -> {
                    execute(dbVersion) {
                        val migrationInstance = migration ?: initMigrations(dbVersion)
                        if (!it) migration = migrationInstance
                        updateEntities(DbEntity.TRIGGER, migrationInstance)
                    }
                }

                "new-major" -> {
                    executeProfileDefault(option, dbVersion) {
                        if (migration == null) migration = initMigrations(dbVersion)
                        val version = DbVersion.of(migration!!.version).nextMajor().toString()
                        newVersion(dbPath, version)
                    }
                }

                "new-minor" -> {
                    executeProfileDefault(option, dbVersion) {
                        if (migration == null) migration = initMigrations(dbVersion)
                        val version = DbVersion.of(migration!!.version).nextMinor().toString()
                        newVersion(dbPath, version)
                    }
                }

                "new-patch" -> {
                    executeProfileDefault(option, dbVersion) {
                        if (migration == null) migration = initMigrations(dbVersion)
                        val version = DbVersion.of(migration!!.version).nextPatch().toString()
                        newVersion(dbPath, version)
                    }
                }

                "new-version" -> {
                    executeProfileDefault(option, dbVersion) {
                        val version = dbVersion ?: DbVersion.of((migration ?: initMigrations(dbVersion)).version).nextPatch().toString()
                        newVersion(dbPath, version)
                    }
                }

                "new-migration" -> {
                    if (args.size < i || args[i].isBlank()) {
                        throw IllegalArgumentException("new-migration option requires a non-blank title argument")
                    }
                    val title = args[i++].trim()

                    executeProfileDefault(option, dbVersion) {
                        if (migration == null) migration = initMigrations(dbVersion)
                        val version = dbVersion ?: migration!!.version
                        newEntityFile(DbEntity.MIGRATION, dbPath, version, title)
                    }
                }

                "new-function" -> {
                    if (args.size < i || args[i].isBlank()) {
                        throw IllegalArgumentException("new-function option requires a non-blank name argument")
                    }
                    val title = args[i++].trim()
                    executeProfileDefault(option, dbVersion) {
                        if (migration == null) migration = initMigrations(dbVersion)
                        val version = dbVersion ?: migration!!.version
                        newEntityFile(DbEntity.FUNCTION, dbPath, version, title)
                    }
                }

                "new-procedure" -> {
                    if (args.size < i || args[i].isBlank()) {
                        throw IllegalArgumentException("new-procedure option requires a non-blank name argument")
                    }
                    val title = args[i++].trim()
                    executeProfileDefault(option, dbVersion) {
                        if (migration == null) migration = initMigrations(dbVersion)
                        val version = dbVersion ?: migration!!.version
                        newEntityFile(DbEntity.PROCEDURE, dbPath, version, title)
                    }
                }

                "new-trigger" -> {
                    if (args.size < i || args[i].isBlank()) {
                        throw IllegalArgumentException("new-trigger option requires a non-blank name argument")
                    }
                    val title = args[i++].trim()
                    executeProfileDefault(option, dbVersion) {
                        if (migration == null) migration = initMigrations(dbVersion)
                        val version = dbVersion ?: migration!!.version
                        newEntityFile(DbEntity.TRIGGER, dbPath, version, title)
                    }
                }

                "new-view" -> {
                    if (args.size < i || args[i].isBlank()) {
                        throw IllegalArgumentException("new-view option requires a non-blank name argument")
                    }
                    val title = args[i++].trim()
                    executeProfileDefault(option, dbVersion) {
                        if (migration == null) migration = initMigrations(dbVersion)
                        val version = dbVersion ?: migration!!.version
                        newEntityFile(DbEntity.VIEW, dbPath, version, title)
                    }
                }

                "exit" -> {
                    exitProcess(1)
                }

                else -> {
                    throw IllegalArgumentException("db option $option is not recognized")
                }
            }
        }
    }

    fun execute(version: String?, command: (multi: Boolean) -> Unit) {
        if (dbProfile == null) {
            if (version != null) {
                dbProfile = DEFAULT_PROFILE
                transaction {
                    command.invoke(false)
                }
            } else {
                val dbProfiles = getDbProfiles()
                dbProfiles.forEach { dbProfileName ->
                    dbProfile = dbProfileName
                    transaction {
                        command.invoke(true)
                    }
                    migration = null
                }
            }
            dbProfile = null
        } else {
            transaction {
                command.invoke(false)
            }
        }
    }

    fun executeProfile(option: String, version: String?, command: () -> Unit) {
        if (dbProfile == null) {
            throw IllegalArgumentException("$option option requires a specific profile")
        } else {
            transaction {
                command.invoke()
            }
        }
    }

    fun executeProfileDefault(option: String, version: String?, command: () -> Unit) {
        if (dbProfile == null) {
            dbProfile = DEFAULT_PROFILE
        }

        transaction {
            command.invoke()
        }
    }

    fun transaction(command: () -> Unit) {
        session.transaction { tx ->
            try {
                command.invoke()
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

