package com.vladsch.kotlin.jdbc

abstract class DbEntityCaseFixer(entities: List<String>) : DbEntityFixer {
    val entityCaseMap = HashMap<String, String>()

    val caseFixRegex: Regex by lazy {
        val sb = StringBuilder()
        var sep = ""
        addRegExPrefix(sb)
        sb.append("(")
        entities.forEach { entityName ->
            val lowerCase = entityName.toLowerCase()
            if (lowerCase != entityName) {
                entityCaseMap.put(lowerCase, entityName)
                sb.append(sep)
                sep = "|"
                addEntityNameRegEx(sb, lowerCase)
            }
        }
        sb.append(")")
        addRegExSuffix(sb)
        sb.toString().toRegex(RegexOption.IGNORE_CASE)
    }

    open fun addRegExPrefix(sb: StringBuilder) {
        sb.append("`")
    }

    open fun addRegExSuffix(sb: StringBuilder) {
        sb.append("`")
    }

    open fun addEntityNameRegEx(sb: StringBuilder, entityName: String) {
        sb.append("\\Q").append(entityName).append("\\E")
    }

    override fun cleanScript(createScript: String): String {
        val fixedCaseSql = createScript.replace(caseFixRegex) { matchResult ->
            val tableName = matchResult.groupValues[1]
            val fixedCase = entityCaseMap[tableName.toLowerCase()]
            if (fixedCase != null) {
                matchResult.value.replace(tableName, fixedCase)
            } else matchResult.value
        }
        return fixedCaseSql
    }
}
