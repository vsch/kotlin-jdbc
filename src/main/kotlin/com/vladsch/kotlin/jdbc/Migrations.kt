package com.vladsch.kotlin.jdbc

import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileWriter

class Migrations(val session: Session, val dbEntityExtractor: DbEntityExtractor, val resourceClass: Class<*>) {
    companion object {
        private val logger = LoggerFactory.getLogger(Migrations::class.java)!!
        val MIGRATIONS_FILE_NAME = DbEntity.TABLE.addSuffix("migrations")
    }

    var verbose = false

    fun getVersions(): List<String> {
        val files = getResourceFiles(resourceClass, "/db").filter { it.matches(DbVersion.regex) }.map { it.toUpperCase() }.sorted()
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
        forEachEntityFile(dbDir, dbVersion, { tableFile ->
            tableFile.delete()
        }, DbEntity.TABLE)

        forEachEntity(DbEntity.TABLE, dbDir, dbVersion, { tableFile, tableScript ->
            if (tableFile.name != MIGRATIONS_FILE_NAME) {
                val tableWriter = FileWriter(tableFile)
                tableWriter.write(tableScript)
                tableWriter.flush()
                tableWriter.close()
            }
        })
    }

    fun doesResourceExist(resourcePath: String): Boolean {
        return getResourceAsStream(resourceClass, resourcePath) != null
    }

    fun equalWithOutOfOrderLines(text1: String, text2: String): Boolean {
        val textLines1 = text1.split('\n').filter { !it.isBlank() }.map { it.trim().removeSuffix(",").trim() }
        val textLines2 = text2.split('\n').filter { !it.isBlank() }.map { it.trim().removeSuffix(",").trim() }
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
                    logger.error(s)
                    if (errorAppendable != null) {
                        errorAppendable.appendln(s)
                    }
                    validationPassed = false
                } else if (tableName + entity.fileSuffix != tableFile.name) {
                    val s = "File ${tableFile.path} for ${entity.displayName} $tableName should be named $tableName${entity.fileSuffix}"
                    logger.error(s)
                    if (errorAppendable != null) {
                        errorAppendable.appendln(s)
                    }
                    validationPassed = false
                }
            }
        }

        forEachEntity(entity, dbVersion, { tableFile, tableScript ->
            if (tableFile.name != MIGRATIONS_FILE_NAME) {
                tableSet.add(tableFile.path)

                if (doesResourceExist(tableFile.path)) {
                    val tableSql = getResourceAsString(resourceClass, tableFile.path)
                    val tableName = entity.extractEntityName(dbEntityExtractor, tableSql)
                    if (tableName == null) {
                        val s = "Invalid SQL ${entity.displayName} file ${tableFile.path}, cannot find ${entity.displayName} name"
                        logger.error(s)
                        if (errorAppendable != null) {
                            errorAppendable.appendln(s)
                        }
                        validationPassed = false
                    } else if (tableName + entity.fileSuffix != tableFile.name) {
                        val s = "File ${tableFile.path} for ${entity.displayName} $tableName should be named $tableName${entity.fileSuffix}"
                        logger.error(s)
                        if (errorAppendable != null) {
                            errorAppendable.appendln(s)
                        }
                        validationPassed = false
                    } else {
                        if (tableSql.trim() != tableScript.trim()) {
                            // see if rearranging lines will make them equal
                            if (!equalWithOutOfOrderLines(tableSql, tableScript)) {
                                val s = "Table validation failed for ${tableFile.path}, database and resource differ"
                                if (validationPassed) {
                                    val tmp = 0
                                }
                                if (verbose) logger.error(s)
                                if (errorAppendable != null) {
                                    if (!verbose) logger.error(s)
                                    errorAppendable.appendln(s)
                                }
                                validationPassed = false
                            }
                        }
                    }
                } else {
                    val s = "Table validation failed for ${tableFile.path}, resource is missing"
                    if (verbose) logger.error(s)
                    if (errorAppendable != null) {
                        if (!verbose) logger.error(s)
                        errorAppendable.appendln(s)
                    }
                    validationPassed = false
                }
            }
        })

        if (validationPassed) {
            forEachEntityResourceFile(DbEntity.TABLE, dbVersion, { tableFile ->
                if (tableFile.name != MIGRATIONS_FILE_NAME) {
                    if (!tableSet.contains(tableFile.path)) {
                        val s = "Table validation failed for ${tableFile.path}, no database table for resource"
                        if (verbose) logger.error(s)
                        if (errorAppendable != null) {
                            if (!verbose) logger.error(s)
                            errorAppendable.appendln(s)
                        }
                        validationPassed = false
                    }
                }
            })
        }

        return validationPassed
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

    private fun runEntitiesSql(migration: MigrationSession, entities: Map<String, DbEntity.EntityData>, opType: String, dropEntityIfExists: String? = null, excludeFilter: ((String) -> Boolean)? = null) {
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
        return session.first(sqlQuery("""
SELECT version FROM migrations
WHERE rolled_back_id IS NULL AND last_problem IS NULL AND script_name IN ('<migrate>', '<rollback>')
ORDER BY migration_id DESC
LIMIT 1
""")) { row ->
            row.string(1)
        } ?: session.first(sqlQuery("""
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
        val tables = dbEntityExtractor.getDbEntities(entity, session)

        if (tables.filter { it.toLowerCase() == "migrations" }.isEmpty()) {
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
                    logger.info("Matched version $latestMatchedVersion based on table schema")
                } else {
                    logger.info("No version matched based on table schema, setting version to V0_0_0")
                }
            }

            val useDbVersion = dbVersion ?: "V0_0_0"

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
            migration = MigrationSession(batchId + 1, dbVersion ?: getLatestVersion(), this)
        }

        return migration
    }

    fun migrate(migration: MigrationSession) {
        // here need to apply up migrations from current version to given version or latest in version sorted order
        val entity = DbEntity.MIGRATION;
        val currentVersion = getCurrentVersion() ?: "V0_0_0"

        val versionCompare = currentVersion.versionCompare(migration.version)
        if (versionCompare > 0) {
            logger.info("Migrate: requested version ${migration.version} is less than current version $currentVersion, use rollback instead")
            return
        } else if (versionCompare <= 0) {
            // need to run all up migrations from current version which have not been run
            val migrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, currentVersion).values.toList().sortedWith(DbEntity.MIGRATIONS_COMPARATOR)

            if (!migrations.isEmpty()) {
                val appliedMigrations = migration.getVersionBatchesNameMap()

                migrations.forEach { entityScript ->
                    val migrationScriptPath = entityScript.entityResourcePath
                    val currentMigration = migration.withVersion(currentVersion)

                    if (!appliedMigrations.containsKey(entityScript.entityResourcePath)) {
                        // apply the migration
                        val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                        currentMigration.insertUpMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                            logger.info("Migrate ${entityScript.entityResourcePath}")
                            runBatchScript(entity, currentMigration, migrationScriptPath, appliedMigrations, sqlScript, entityScript)
                        }
                    }
                }
            } else {
                logger.debug("Migrate: no migrations in current version $currentVersion")
            }

            if (versionCompare < 0) {
                // need to run all migrations from later versions up to requested version
                val versionList = getVersions()
                    .filter { it.compareTo(currentVersion) > 0 && (it.versionCompare(migration.version) <= 0) }
                    .sortedWith(Comparator(String::versionCompare))

                versionList.forEach { version ->
                    val versionMigrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, version)
                        .values.toList()
                        .sortedWith(DbEntity.MIGRATIONS_COMPARATOR)

                    val versionMigration = migration.withVersion(version)

                    if (!versionMigrations.isEmpty()) {
                        versionMigrations.forEach { entityScript ->
                            // apply the migration
                            val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                            versionMigration.insertUpMigrationAfter(entityScript.entityResourcePath, sqlScript) {
                                logger.info("Migrate ${entityScript.entityResourcePath}")
                                runBatchScript(entity, versionMigration, entityScript.entityResourcePath, null, sqlScript, entityScript)
                            }
                        }
                    } else {
                        logger.debug("Migrate: no up migrations in version $version")
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
                val migrationPartName = "$migrationScriptPath[${index}:$startLine-${line - 1}]"
                if (opType == DbEntity.MIGRATION) {
                    if (appliedMigrations == null || !appliedMigrations.containsKey(migrationPartName)) {
                        migration.insertUpMigrationAfter(migrationPartName, sql) {
                            logger.info("Migrate ${entityData.entityResourcePath} part [${index}:$startLine-${line - 1}]")
                            session.execute(sqlQuery(sql))
                        }
                    }
                } else {
                    if (appliedMigrations == null || appliedMigrations.containsKey(migrationPartName)) {
                        migration.insertDownMigrationAfter(migrationPartName, sql) {
                            logger.info("Rollback ${entityData.entityResourcePath} part [${index}:$startLine-${line - 1}]")
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
                val migrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, currentVersion)
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
                                logger.info("Rollback ${entityScript.entityResourcePath}")
                                runBatchScript(entity, currentMigration, entityScript.entityResourcePath, null, sqlScript, entityScript)
                            }
                        }
                    }
                } else {
                    logger.debug("Rollback: no down migrations in current version $currentVersion")
                }

                if (versionCompare > 0) {
                    // need to run all migrations from earlier versions down up to but not including requested version
                    val versionList = getVersions()
                        .filter { it.compareTo(currentVersion) < 0 && it.compareTo(migration.version) > 0 }
                        .sortedWith(Comparator(String::versionCompare))
                        .reversed()

                    versionList.forEach { version ->
                        val versionMigrations = entity.getEntityResourceScripts(resourceClass, dbEntityExtractor, version)
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
                                    logger.info("Rollback ${entityScript.entityResourcePath}")
                                    runBatchScript(entity, versionMigration, migrationScriptPath, null, sqlScript, entityScript)
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
                    logger.debug("Rollback: rolled back to start of history at pre-migrations for ${migration.version}")
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
        val versionDir = getVersionDirectory(dbDir, dbVersion, false)

        val schemaDir = dbDir + "schema"
        schemaDir.ensureCreateDirectory("dbDir/schema")
        val versionFile = schemaDir + "version.txt"
        versionFile.writeText("""# Version: ${dbVersion}""".trimIndent())

        val functionsDir = schemaDir + DbEntity.FUNCTION.dbEntityDirectory
        //val migrationsDir = schemaDir + DbEntity.MIGRATION.dbEntityDirectory
        val proceduresDir = schemaDir + DbEntity.PROCEDURE.dbEntityDirectory
        val tablesDir = schemaDir + DbEntity.TABLE.dbEntityDirectory
        val triggerDir = schemaDir + DbEntity.TRIGGER.dbEntityDirectory
        val viewsDir = schemaDir + DbEntity.VIEW.dbEntityDirectory

        functionsDir.ensureCreateDirectory("db/schema/" + DbEntity.FUNCTION.dbEntityDirectory)
        // migrationsDir.ensureCreateDirectory("db/schema/" + DbEntity.MIGRATION.dbEntityDirectory)
        proceduresDir.ensureCreateDirectory("db/schema/" + DbEntity.PROCEDURE.dbEntityDirectory)
        tablesDir.ensureCreateDirectory("db/schema/" + DbEntity.TABLE.dbEntityDirectory)
        triggerDir.ensureCreateDirectory("db/schema/" + DbEntity.TRIGGER.dbEntityDirectory)
        viewsDir.ensureCreateDirectory("db/schema/" + DbEntity.VIEW.dbEntityDirectory)

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
        val versionDir = getVersionDirectory(dbDir, dbVersion, null)

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

        functionsDir.ensureCreateDirectory("db/$dbVersion/" + DbEntity.FUNCTION.dbEntityDirectory)
        migrationsDir.ensureCreateDirectory("db/$dbVersion/" + DbEntity.MIGRATION.dbEntityDirectory)
        proceduresDir.ensureCreateDirectory("db/$dbVersion/" + DbEntity.PROCEDURE.dbEntityDirectory)
        tablesDir.ensureCreateDirectory("db/$dbVersion/" + DbEntity.TABLE.dbEntityDirectory)
        triggerDir.ensureCreateDirectory("db/$dbVersion/" + DbEntity.TRIGGER.dbEntityDirectory)
        viewsDir.ensureCreateDirectory("db/$dbVersion/" + DbEntity.VIEW.dbEntityDirectory)

        // copy all entities from previous version except migrations
        val previousVersion = getPreviousVersion(dbVersion)
        if (previousVersion != null) {
            copyEntities(DbEntity.FUNCTION, getVersionDirectory(dbDir, previousVersion, true), versionDir, true)
            //            copyEntities(DbEntity.MIGRATION, getVersionDirectory(dbDir, previousVersion, true), versionDir)
            //            copyEntities(DbEntity.ROLLBACK, getVersionDirectory(dbDir, previousVersion, true), versionDir)
            copyEntities(DbEntity.PROCEDURE, getVersionDirectory(dbDir, previousVersion, true), versionDir, true)
            copyEntities(DbEntity.TABLE, getVersionDirectory(dbDir, previousVersion, true), versionDir, true) { it.toLowerCase() == MIGRATIONS_FILE_NAME }
            copyEntities(DbEntity.TRIGGER, getVersionDirectory(dbDir, previousVersion, true), versionDir, true)
            copyEntities(DbEntity.VIEW, getVersionDirectory(dbDir, previousVersion, true), versionDir, true)
        }
    }

    fun newEntityFile(entity: DbEntity, dbDir: File, dbVersion: String, entityName: String) {
        val versionDir = getVersionDirectory(dbDir, dbVersion, false)

        val entityDir = versionDir + entity.dbEntityDirectory

        entityDir.ensureCreateDirectory("db/$dbVersion/${entity.dbEntityDirectory}")

        if (entity == DbEntity.MIGRATION || entity == DbEntity.ROLLBACK) {
            var lastMigration: Int = 0

            entity.getEntityFiles(entityDir).forEach {
                val (num, name) = File(it).name.extractLeadingDigits()
                if (num != null && num > lastMigration) {
                    lastMigration = num
                }
            }

            entity.getEntityFiles(entityDir).forEach {
                val (num, name) = File(it).name.extractLeadingDigits()
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
        } else {
            val entityFile = entityDir + "$entityName${entity.fileSuffix}"

            val entitySample = entity.getEntitySample(dbDir, resourceClass)
            entityFile.writeText(entitySample.replace("__VERSION__".toRegex(), dbVersion).replace("__NAME__".toRegex(), entityName))
        }
    }

    fun newEvolution(evolutionsDir: File, dbVersion: String) {
        evolutionsDir.ensureExistingDirectory("evolutions path")

        logger.info("Generated new play evolution in ${evolutionsDir.path}")

        val sb = StringBuilder()
        val versionMigrations = DbEntity.MIGRATION.getEntityResourceScripts(resourceClass, dbEntityExtractor, dbVersion)
            .values.toList()
            .sortedWith(DbEntity.MIGRATIONS_COMPARATOR)

        val cleanComment = "(?<=^|\n)#".toRegex()

        if (!versionMigrations.isEmpty()) {
            sb.appendln("# --- !Ups")
            versionMigrations.forEach { entityScript ->
                // apply the migration
                val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                logger.info("Adding to !Ups ${entityScript.entityResourcePath}")
                sb.append("-- ").appendln(entityScript.entityResourcePath)
                sb.appendln(sqlScript.replace(cleanComment, "-- "))
            }
        } else {
            logger.info("Migrate: no up migrations in version $dbVersion")
        }

        val versionRollbacks = DbEntity.ROLLBACK.getEntityResourceScripts(resourceClass, dbEntityExtractor, dbVersion)
            .values.toList()
            .sortedWith(DbEntity.MIGRATIONS_COMPARATOR)
            .reversed()

        if (!versionRollbacks.isEmpty()) {
            sb.appendln("# --- !Downs")
            versionRollbacks.forEach { entityScript ->
                // apply the migration
                val sqlScript = getResourceAsString(resourceClass, entityScript.entityResourcePath)
                logger.info("Adding to !Downs ${entityScript.entityResourcePath}")
                sb.append("-- ").appendln(entityScript.entityResourcePath)
                sb.appendln(sqlScript.replace(cleanComment, "-- "))
            }
        } else {
            logger.info("Migrate: no down migrations in version $dbVersion")
        }

        if (!sb.isEmpty()) {
            var lastMigration: Int = 0

            evolutionsDir.list { dir, name ->
                val (num, ext) = name.extractLeadingDigits()
                if (num != null && ext == ".sql" && num > lastMigration) {
                    lastMigration = num
                }
                false
            }

            lastMigration++

            val evolutionFile = evolutionsDir + "$lastMigration.sql"
            evolutionFile.writeText(sb.toString())
            logger.info("Generated new play evolution $lastMigration.sql in ${evolutionsDir.path}")
        } else {
            logger.info("No migrations in $dbVersion for generating play evolution")
        }
    }

    /**
     * Execute db command
     *
     * @param args Array<String>
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
        var dbPath: File = File(System.getProperty("user.dir"))

        var migration: MigrationSession? = null

        session.transaction { tx ->
            try {
                var i = 0
                while (i < args.size) {
                    val option = args[i++]
                    when (option) {
                        "-v" -> {
                            verbose = true
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

                        "new-evolution" -> {
                            if (args.size < i) {
                                throw IllegalArgumentException("new-evolution option requires a path argument")
                            }
                            val path = args[i++]
                            val pathDir = File(path).ensureExistingDirectory("evolutions path")
                            if (dbVersion == null) dbVersion = getCurrentVersion()

                            if (dbVersion != null) {
                                newEvolution(pathDir, dbVersion!!)
                            } else {
                                throw IllegalArgumentException("new-evolution option requires a database which has a current migration version")
                            }
                        }

                        "dump-tables" -> {
                            if (dbVersion == null) dbVersion = getCurrentVersion()
                            dumpTables(dbPath, dbVersion!!)
                        }

                        "update-schema" -> {
                            if (dbVersion == null) dbVersion = getCurrentVersion()
                            updateSchema(dbPath, dbVersion!!)
                        }

                        "validate-tables", "validate" -> {
                            if (dbVersion == null) dbVersion = getCurrentVersion()
                            validateTableResourceFiles(dbVersion!!)
                        }

                        "init" -> {
                            if (migration != null) {
                                throw IllegalArgumentException("db init command must be first executed command")
                            }
                            migration = initMigrations(dbVersion)
                        }

                        "migrate" -> {
                            // here need to apply up migrations from current version to given version or latest in version sorted order
                            if (migration == null) migration = initMigrations(dbVersion)

                            migrate(migration!!)
                        }

                        "rollback" -> {
                            // here need to apply down migrations from current version to given version or if none given then rollback the last batch which was not rolled back
                            if (migration == null) migration = initMigrations(dbVersion ?: getPreviousVersion(getCurrentVersion()
                                ?: "V0_0_0"))

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

                        "new-major" -> {
                            if (migration == null) migration = initMigrations(dbVersion)
                            val version = DbVersion.of(migration!!.version).nextMajor().toString()
                            newVersion(dbPath, version)
                        }

                        "new-minor" -> {
                            if (migration == null) migration = initMigrations(dbVersion)
                            val version = DbVersion.of(migration!!.version).nextMinor().toString()
                            newVersion(dbPath, version)
                        }

                        "new-patch" -> {
                            if (migration == null) migration = initMigrations(dbVersion)
                            val version = DbVersion.of(migration!!.version).nextPatch().toString()
                            newVersion(dbPath, version)
                        }

                        "new-version" -> {
                            val version = dbVersion ?: DbVersion.of((migration ?: initMigrations(dbVersion)).version).nextPatch().toString()
                            newVersion(dbPath, version)
                        }

                        "new-migration" -> {
                            if (args.size < i || args[i].isBlank()) {
                                throw IllegalArgumentException("new-migration option requires a non-blank title argument")
                            }
                            val title = args[i++].trim()
                            if (migration == null) migration = initMigrations(dbVersion)
                            val version = dbVersion ?: migration!!.version
                            newEntityFile(DbEntity.MIGRATION, dbPath, version, title)
                        }

                        "new-function" -> {
                            if (args.size < i || args[i].isBlank()) {
                                throw IllegalArgumentException("new-function option requires a non-blank name argument")
                            }
                            val title = args[i++].trim()
                            if (migration == null) migration = initMigrations(dbVersion)
                            val version = dbVersion ?: migration!!.version
                            newEntityFile(DbEntity.FUNCTION, dbPath, version, title)
                        }

                        "new-procedure" -> {
                            if (args.size < i || args[i].isBlank()) {
                                throw IllegalArgumentException("new-procedure option requires a non-blank name argument")
                            }
                            val title = args[i++].trim()
                            if (migration == null) migration = initMigrations(dbVersion)
                            val version = dbVersion ?: migration!!.version
                            newEntityFile(DbEntity.PROCEDURE, dbPath, version, title)
                        }

                        "new-trigger" -> {
                            if (args.size < i || args[i].isBlank()) {
                                throw IllegalArgumentException("new-trigger option requires a non-blank name argument")
                            }
                            val title = args[i++].trim()
                            if (migration == null) migration = initMigrations(dbVersion)
                            val version = dbVersion ?: migration!!.version
                            newEntityFile(DbEntity.TRIGGER, dbPath, version, title)
                        }

                        "new-view" -> {
                            if (args.size < i || args[i].isBlank()) {
                                throw IllegalArgumentException("new-view option requires a non-blank name argument")
                            }
                            val title = args[i++].trim()
                            if (migration == null) migration = initMigrations(dbVersion)
                            val version = dbVersion ?: migration!!.version
                            newEntityFile(DbEntity.VIEW, dbPath, version, title)
                        }

                        "exit" -> {
                            tx.commit()
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

