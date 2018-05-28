package com.vladsch.kotlin.jdbc

import java.io.File
import java.io.FilenameFilter

@Suppress("MemberVisibilityCanBePrivate")
enum class DbEntity(val dbEntity: String, val displayName: String, val dbEntityDirectory: String, val fileSuffix: String) {

    FUNCTION("FUNCTION", "function", "functions", ".udf.sql"),
    PROCEDURE("PROCEDURE", "procedure", "procedures", ".prc.sql"),
    TABLE("TABLE", "table", "tables", ".tbl.sql"),
    TRIGGER("TRIGGER", "insert trigger", "triggers", ".trg.sql"),
    VIEW("VIEW", "view", "views", ".view.sql"),
    MIGRATION("", "migration", "migrations", ".up.sql"),
    ROLLBACK("", "rollback", "migrations", ".down.sql"),
    ;

    fun getEntityDirectory(dbVersionDir: File, createDir: Boolean?): File {
        if (createDir != null) {
            dbVersionDir.ensureExistingDirectory("dbDir/dbVersion")
        }

        // may need to create table directory
        val entityDir = dbVersionDir + this.dbEntityDirectory

        if (createDir == true) {
            entityDir.ensureCreateDirectory("dbDir/dbVersion/${this.dbEntityDirectory}")
        } else if (createDir == false) {
            entityDir.ensureExistingDirectory("dbDir/dbVersion/${this.dbEntityDirectory}")
        }
        return entityDir
    }

    fun removeSuffix(fileName: String): String {
        return fileName.removeSuffix(fileSuffix)
    }

    fun addSuffix(fileName: String): String {
        return fileName + fileSuffix
    }

    fun getEntityDirectory(dbDir: File, dbVersion: String, createDir: Boolean?): File {
        val dbVersionDir = getVersionDirectory(dbDir, dbVersion, createDir)
        return getEntityDirectory(dbVersionDir, createDir)
    }

    fun getEntityResourceDirectory(dbVersion: String): File {
        val dbVersionDir = File("/db") + dbVersion

        // may need to create table directory
        val entityDir = dbVersionDir + this.dbEntityDirectory

        return entityDir
    }

    fun getEntitySample(dbDir: File, resourceClass: Class<*>): String {
        val templateFile = ((dbDir + "templates") + dbEntityDirectory) + getEntitySampleName()
        var sampleText: String? = null

        if (templateFile.exists()) {
            sampleText = getFileContent(templateFile)
        }

        if (sampleText == null) {
            val samplesResourcePath = "/sample-db/V0_0_0/$dbEntityDirectory/${getEntitySampleName()}"
            sampleText = getResourceAsString(resourceClass, samplesResourcePath)
        }
        return sampleText
    }

    fun isEntityFile(file: String): Boolean {
        return file.endsWith(this.fileSuffix)
    }

    fun isEntityFile(file: File): Boolean {
        return file.name.endsWith(this.fileSuffix)
    }

    fun getEntitySampleName(): String {
        return if (this === MIGRATION || this === ROLLBACK) {
            "0.sample$fileSuffix"
        } else {
            "sample$fileSuffix"
        }
    }

    fun getEntityName(fileName: String): String {
        var entityName = fileName

        if (this === MIGRATION || this === ROLLBACK) {
            // has optional leading count with .
            val (count, name) = fileName.extractLeadingDigits()
            entityName = name
        }

        return entityName.substring(0, entityName.length - this.fileSuffix.length)
    }

    fun getEntityName(file: File): String {
        return getEntityName(file.name)
    }

    fun getEntityFiles(entityDir: File): List<String> {
        val entityList = ArrayList<String>()
        if (entityDir.exists()) {
            entityDir.list(FilenameFilter { file, name -> name.endsWith(this.fileSuffix) })
                .map { entityDir + it }
                .filter { it.isFile }
                .forEach {
                    entityList.add(it.path)
                }
        }
        return entityList
    }

    fun getEntityFiles(dbDir: File, dbVersion: String, createDir: Boolean? = true): List<String> {
        val entityDir = this.getEntityDirectory(dbDir, dbVersion, createDir)
        if (createDir == true) {
            entityDir.ensureCreateDirectory("dbDir/dbVersion/${this.dbEntityDirectory}")
        } else if (createDir == false) {
            entityDir.ensureExistingDirectory("dbDir/dbVersion/${this.dbEntityDirectory}")
        } else {
            return listOf()
        }

        return getEntityFiles(entityDir)
    }

    fun getEntityResourceFiles(resourceClass: Class<*>, dbVersion: String): List<String> {
        val entityDir = this.getEntityResourceDirectory(dbVersion)
        return getResourceFiles(resourceClass, entityDir.path)
    }

    fun getEntityFile(entityDir: File, entityName: String): File {
        return entityDir + (entityName + this.fileSuffix)
    }

    fun getEntityFile(dbDir: File, dbVersion: String, entityName: String, createDir: Boolean? = true): File {
        val entityDir = getEntityDirectory(dbDir, dbVersion, createDir)
        return getEntityFile(entityDir, entityName)
    }

    data class EntityData(val entityName: String, val entityResourceName: String, val entityResourcePath: String, val entitySql: String)

    object MIGRATIONS_COMPARATOR : Comparator<DbEntity.EntityData> {
        override fun compare(o1: DbEntity.EntityData?, o2: DbEntity.EntityData?): Int {
            val (num1, name1) = o1?.entityResourceName.extractLeadingDigits()
            val (num2, name2) = o2?.entityResourceName.extractLeadingDigits()

            return if (num1 != null && num2 != null) {
                val nums = num1.compareTo(num2)
                if (nums == 0) name1.compareTo(name2)
                else nums
            } else {
                name1.compareTo(name2)
            }
        }
    }

    /**
     * Get entity to EntityScript
     *
     * @return Map<String,EntityScript>
     */
    fun getEntityResourceScripts(resourceClass: Class<*>, dbEntityExtractor: DbEntityExtractor, entityDir: File): Map<String, EntityData> {
        val entityNameRegEx = dbEntityExtractor.getExtractEntityNameRegEx(this);
        val files = getResourceFiles(resourceClass, entityDir.path)

        val entities = HashMap<String, EntityData>()
        for (file in files) {
            if (file.endsWith(fileSuffix)) {
                val entityNameFile = file.substring(0, file.length - this.fileSuffix.length)
                val entitySql = getResourceAsString(resourceClass, (entityDir + file).path)
                if (entityNameRegEx == null) {
                    // no name check
                    val entityName = this.removeSuffix(entityNameFile)
                    val entityLowercaseName = entityName.toLowerCase()
                    entities[entityLowercaseName] = EntityData(entityName, file, (entityDir + file).path, entitySql);
                } else {
                    val matchGroup = entityNameRegEx.find(entitySql)
                    if (matchGroup != null) {
                        val entityName = matchGroup.groupValues[1].removeSurrounding("`")
                        val entityLowercaseName = entityName.toLowerCase()

                        if (entityName != entityNameFile) {
                            throw IllegalStateException("File $file for $displayName $entityName should be named $entityName.udf.sql")
                        }

                        if (entities.contains(entityLowercaseName)) {
                            throw IllegalStateException("Duplicate SQL $displayName in File $file, $entityName already defined in ${entities[entityName]}")
                        }

                        entities[entityLowercaseName] = EntityData(entityName, file, (entityDir + file).path, entitySql);
                    } else {
                        throw IllegalStateException("Invalid SQL $displayName file $file, cannot find $displayName name")
                    }
                }
            }
        }
        return entities
    }

    fun getEntityResourceScripts(resourceClass: Class<*>, dbEntityExtractor: DbEntityExtractor, dbVersion: String): Map<String, EntityData> {
        val entityDir = getEntityDirectory(File("/db"), dbVersion, null)
        return getEntityResourceScripts(resourceClass, dbEntityExtractor, entityDir)
    }
}
