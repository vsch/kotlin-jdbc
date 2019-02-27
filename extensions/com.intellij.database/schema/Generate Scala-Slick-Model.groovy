import com.intellij.database.model.DasColumn
import com.intellij.database.model.DasObject
import com.intellij.database.model.DasTable
import com.intellij.database.model.ObjectKind
import com.intellij.database.util.Case
import com.intellij.database.util.DasUtil
import com.intellij.openapi.util.text.StringUtil
import com.intellij.psi.codeStyle.NameUtil
import groovy.json.JsonSlurper

/*
 * Available context bindings:
 *   SELECTION   Iterable<DasObject>
 *   PROJECT     project
 *   FILES       files helper
 */
DEBUG = false                    // if true output debug trace to debug.log
classFileNameSuffix = "Model"   // appended to class file name
snakeCaseTables = false         // if true convert snake_case table names to Pascal case, else leave as is
indentSpaces = "  "           // spaces for each indent level
fileExtension = ".scala"
convertTimeBasedToString = true   // to convert all date, time and timestamp to String in the model

// column names marked as boolean when tinyint, only needed if using jdbc introspection which does not report actual declared type so all tinyint are tinyint(3)
//forceBooleanTinyInt = (~/^(?:deleted|checkedStatus|checked_status|optionState|option_state)$/)
forceBooleanTinyInt = ""

downsizeLongIdToInt = true // if true changes id columns which would be declared Long to Int, change this to false to leave them as Long

typeMapping = convertTimeBasedToString ? [
        (~/(?i)tinyint\(1\)/)       : "Boolean",
        (~/(?i)tinyint/)            : "TinyInt",     // changed to Int if column name not in forceBooleanTinyInt
        (~/(?i)bigint/)             : "Long",
        (~/(?i)int/)                : "Int",
        (~/(?i)float/)              : "Float",
        (~/(?i)double|decimal|real/): "Double",
        (~/(?i)datetime|timestamp/) : "String",
        (~/(?i)date/)               : "String",
        (~/(?i)time/)               : "String",
        (~/(?i)/)                   : "String"
] : [
        (~/(?i)tinyint\(1\)/)       : "Boolean",
        (~/(?i)tinyint/)            : "TinyInt",     // changed to Int if column name not in forceBooleanTinyInt
        (~/(?i)bigint/)             : "Long",
        (~/(?i)int/)                : "Int",
        (~/(?i)float/)              : "Float",
        (~/(?i)double|decimal|real/): "Double",
        (~/(?i)datetime|timestamp/) : "Timestamp",
        (~/(?i)date/)               : "Date",
        (~/(?i)time/)               : "Time",
        (~/(?i)/)                   : "String"
]

FILES.chooseDirectoryAndSave("Choose directory", "Choose where to store generated files") { File dir ->
    // read in possible map of tables to subdirectories
    def tableMap = null
    String packagePrefix = ""
    String removePrefix = ""
    boolean skipUnmapped = false
    File exportDir = dir
    File mapFile = null
    File projectDir = null

    def File tryDir = dir
    while (tryDir != null && tryDir.exists() && tryDir.isDirectory()) {
        def File tryMap = new File(tryDir, "model-config.json")
        if (tryMap.isFile() && tryMap.canRead()) {
            mapFile = tryMap
            exportDir = tryDir
            break
        }

        // see if this directory has .idea, then must be project root
        tryMap = new File(tryDir, ".idea")
        if (tryMap.isDirectory() && tryMap.exists()) {
            projectDir = tryDir
            break
        }

        tryDir = tryDir.parentFile
    }

    String packageName

    if (projectDir != null) {
        packageName = exportDir.path.substring(projectDir.path.length() + 1).replace('/', '.')
        // now drop first part since it is most likely sources root and not part of the package path
        int dot = packageName.indexOf('.')
        if (dot > 0) packageName = packageName.substring(dot + 1)
    } else {
        packageName = "com.sample"
    }

    if (DEBUG) {
        new File(exportDir, "debug.log").withPrintWriter { PrintWriter dbg ->
            dbg.println("exportDir: ${exportDir.path}, mapFile: ${mapFile}")

            if (mapFile != null && mapFile.isFile() && mapFile.canRead()) {
                JsonSlurper jsonSlurper = new JsonSlurper()
                def reader = new BufferedReader(new InputStreamReader(new FileInputStream(mapFile), "UTF-8"))
                data = jsonSlurper.parse(reader)
                if (DEBUG && data != null) {
                    packagePrefix = data["package-prefix"] ?: ""
                    removePrefix = data["remove-prefix"] ?: ""
                    skipUnmapped = data["skip-unmapped"] ?: false
                    tableMap = data["file-map"]

                    dbg.println("package-prefix: '$packagePrefix'")
                    dbg.println ""
                    dbg.println("skip-unmapped: $skipUnmapped")
                    dbg.println ""
                    dbg.println "file-map: {"
                    tableMap.each { dbg.println "    $it" }
                    dbg.println "}"
                    dbg.println ""
                }
            }
            SELECTION.filter { DasObject it -> it instanceof DasTable && it.getKind() == ObjectKind.TABLE }.each { DasTable it ->
                generate(dbg, it, exportDir, tableMap, packageName, packagePrefix, removePrefix, skipUnmapped)
            }
        }
    } else {
        PrintWriter dbg = null

        if (mapFile != null && mapFile.isFile() && mapFile.canRead()) {
            JsonSlurper jsonSlurper = new JsonSlurper()
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(mapFile), "UTF-8"))
            data = jsonSlurper.parse(reader)
            packagePrefix = data["package-prefix"] ?: ""
            removePrefix = data["remove-prefix"] ?: ""
            skipUnmapped = data["skip-unmapped"] ?: false
            tableMap = data["file-map"]
        }

        SELECTION.filter { DasObject it -> it instanceof DasTable && it.getKind() == ObjectKind.TABLE }.each { DasTable it ->
            generate(dbg, it, exportDir, tableMap, packageName, packagePrefix, removePrefix, skipUnmapped)
        }
    }
}

void generate(PrintWriter dbg, DasTable table, File dir, tableMap, String packageName, String packagePrefix, String removePrefix, boolean skipUnmapped) {
    String className = snakeCaseTables ? toJavaName(toSingular(table.getName()), true) : toSingular(table.getName())
    dbg.println("className: ${className}, tableName: ${table.getName()}, singular: ${toSingular(table.getName())}")

    def fields = calcFields(table)
    String fileName = className + "${classFileNameSuffix}$fileExtension"

    def mappedFile = tableMap != null ? tableMap[fileName] : null
    if (mappedFile == null && tableMap != null && tableMap[""] != null) {
        mappedFile = suffixOnceWith(tableMap[""], "/") + fileName
    }

    if (dbg != null) dbg.println "fileName ${fileName} mappedFile ${mappedFile}"

    def file
    if (mappedFile != null && !mappedFile.trim().isEmpty()) {
        file = new File(dir, mappedFile);
        String unprefixed = (removePrefix == "" || !mappedFile.startsWith(removePrefix)) ? mappedFile : mappedFile.substring(removePrefix.length())
        packageName = packagePrefix + new File(unprefixed).parent.replace("/", ".")
    } else {
        file = new File(dir, fileName)
        packageName = packageName
        if (dbg != null && (skipUnmapped || mappedFile != null)) dbg.println "skipped ${fileName}"
        if (skipUnmapped || mappedFile != null) return
    }

    file.withPrintWriter { out -> generateModel(dbg, out, (String) table.getName(), className, fields, packageName) }
}

static String defaultValue(def value) {
    String text = (String) value
    return text == null ? "" : (text == "CURRENT_TIMESTAMP" ? " $text" : " '$text'")
}

def calcFields(table) {
    def colIndex = 0
    DasUtil.getColumns(table).reduce([]) { def fields, DasColumn col ->
        def dataType = col.getDataType()
        def spec = dataType.getSpecification()
        def suffix = dataType.suffix
        def typeStr = (String) typeMapping.find { p, t -> p.matcher(Case.LOWER.apply(spec)).find() }.value
        def colName = (String) col.getName()
        def colNameLower = (String) Case.LOWER.apply(colName)
        colIndex++
        def colType = downsizeLongIdToInt && typeStr == "Long" && DasUtil.isPrimary(col) || DasUtil.isForeign(col) && colNameLower.endsWith("id") ? "Int" : typeStr
        def javaColName = (String) toJavaName(colName, false)
        if (typeStr == "TinyInt") {
            if (forceBooleanTinyInt && javaColName.matches(forceBooleanTinyInt)) {
                colType = "Boolean"
            } else {
                colType = "Int"
            }
        }
        def attrs = col.getTable().getColumnAttrs(col)
        def columnDefault = col.getDefault()
        def isAutoInc = DasUtil.isAutoGenerated(col)
        def isAuto = isAutoInc || columnDefault == "CURRENT_TIMESTAMP" || attrs.contains(DasColumn.Attribute.COMPUTED)
        fields += [[
                           name    : javaColName,
                           column  : colName,
                           type    : colType,
                           suffix  : suffix,
                           col     : col,
                           spec    : spec,
                           attrs   : attrs,
                           default : columnDefault,
//                           constraints : constraints.reduce("") { all, constraint ->
//                               all += "[ name: ${constraint.name}, " + "kind: ${constraint.getKind()}," + "]"
//                           },
                           notNull : col.isNotNull(),
                           autoInc : isAutoInc,
                           nullable: !col.isNotNull() || isAuto || columnDefault != null,
                           key     : DasUtil.isPrimary(col),
                           auto    : isAuto,
                           annos   : ""]]
        fields
    }
}

static String rightPad(String text, int len) {
    def padded = text
    def pad = len - text.length()
    while (pad-- > 0) padded += " "
    return padded
}

static String suffixOnceWith(String str, String suffix) {
    return str.endsWith(suffix) ? str : "$str$suffix";
}

static String lowerFirst(String str) {
    return str.length() > 0 ? Case.LOWER.apply(str[0]) + str[1..-1] : str
}

static String toSingular(String str) {
    String[] s = NameUtil.splitNameIntoWords(str).collect { it }
    String lastSingular = StringUtil.unpluralize(s[-1]) ?: ""
    return str.substring(0, str.length() - s[-1].length()) + lastSingular
}

static String toPlural(String str) {
    String[] s = NameUtil.splitNameIntoWords(str).collect { it }
    String lastPlural = StringUtil.pluralize(s[-1]) ?: ""
    return str.substring(0, str.length() - s[-1].length()) + lastPlural
}

static String toJavaName(String str, boolean capitalize) {
    String s = NameUtil.splitNameIntoWords(str)
            .collect { Case.LOWER.apply(it).capitalize() }
            .join("")
            .replaceAll(/[^\p{javaJavaIdentifierPart}[_]]/, "_")
    (capitalize || s.length() == 1) ? s : Case.LOWER.apply(s[0]) + s[1..-1]
}

void generateModel(PrintWriter dbg, PrintWriter out, String tableName, String className, def fields, String packageName) {
    def dbCase = true
    def keyCount = 0
    def nonKeyCount = 0
    def timestampCount = 0
    def dateCount = 0
    def timeCount = 0
    fields.each() {
        if (it.name != it.column) dbCase = false
        if (it.key) keyCount++
        else nonKeyCount++

        if (it.type == "Timestamp") timestampCount++
        else if (it.type == "Date") dateCount++
        else if (it.type == "Time") timeCount++
    }

    // set single key to nullable and auto
    if (keyCount == 1 && nonKeyCount > 0) {
        fields.each() {
            if (it.key) {
                it.nullable = true
                it.auto = true
            }
        }
    }

    out.println "package $packageName"
    out.println ""

    def timeFields = (timestampCount > 0 ? 1 : 0) + (dateCount > 0 ? 1 : 0) + (timeCount > 0 ? 1 : 0)
    def importSql = "import java.sql."
    def sep = ""
    if (timeFields == 1) {
        importSql += (dateCount > 0 ? "Date" : "") + (timeCount > 0 ? "Time" : "") + (timestampCount > 0 ? "Timestamp" : "")
    } else {
        sep = "{"
        if (dateCount > 0) {
            importSql += sep + "Date"
            sep = ", "
        }
        if (timeCount > 0) {
            importSql += sep + "Time"
            sep = ", "
        }
        if (timestampCount > 0) {
            importSql += sep + "Timestamp"
            sep = ", "
        }
    }
    if (timeFields > 0) {
        out.println importSql
        out.println ""
    }

//    out.println "import ai.x.play.json.Jsonx"
//    out.println "import play.api.db.slick.HasDatabaseConfigProvider"
//    out.println "import play.api.libs.json.OFormat"
    out.println "import play.api.db.slick.HasDatabaseConfigProvider"
    out.println "import play.api.libs.json.{Json, OFormat}"
    out.println "import slick.jdbc.JdbcProfile"
    out.println "import slick.lifted.ProvenShape"
    out.println ""
    out.println "/**"
    sep = "";
    def len = 0
    fields.each() {
        if (len < it.name.length()) len = it.name.length()
    }

    fields.each() {
        out.print "${indentSpaces}* @param "
        if (it.annos != "") out.println "${indentSpaces}${it.annos}"
        out.print "${rightPad(it.name, len)} ${it.nullable ? 'Option[' + it.type + ']' : it.type}"
        out.println ""
    }
//    out.println "  * @param stepInstanceId    Int"
//    out.println "  * @param stepId            Int"
//    out.println "  * @param userUuid          Option[String]"
//    out.println "  * @param comment           Option[String]"
    out.println "  */"

    out.println ""
//    case class StepInstanceModel(
//      stepInstanceId: Option[Int] = None,
//      stepId: Int,
//      processInstanceId: Int,
//      createdAt: Option[String],
//      updatedAt: Option[String],
//      deadlineAt: Option[Date],
//    )
    out.println "case class ${className}Model("
    sep = "";
    fields.each() {
        out.print sep
        out.print "${indentSpaces}${it.name}: ${it.nullable ? 'Option[' + it.type + '] = None' : it.type}"
        sep = ",\n"
        if (it.annos != "") out.print " // ${it.annos}"
    }
    out.println "\n)"
    out.println ""

    out.println "object ${className}Model {"
//    out.println "${indentSpaces}implicit val ${lowerFirst(className)}Format: OFormat[${className}Model] = Jsonx.formatCaseClass[${className}Model]"
    out.println "${indentSpaces}implicit val ${lowerFirst(className)}Format: OFormat[${className}Model] = Json.format[${className}Model]"
    out.println "}"
    out.println ""

    out.println "trait ${className}Component {"
    out.println "${indentSpaces}self: HasDatabaseConfigProvider[JdbcProfile] =>"
    out.println ""
    out.println "${indentSpaces}import profile.api._"
    out.println ""
    out.println "${indentSpaces}class ${className}(tag: Tag) extends Table[${className}Model](tag, \"${tableName}\") {"
    sep = ""
    fields.each() {
        out.print sep
        varType = "${it.nullable ? 'Option[' + it.type + ']' : it.type}"
        out.print "${indentSpaces}${indentSpaces}def ${it.name}: Rep[${varType}] = column[${varType}](\"${it.name}\""
        if (it.key) out.print ", O.PrimaryKey"
        if (it.auto) out.print ", O.AutoInc"
        out.print ")"
        if (it.annos != "") out.print " // ${it.annos}"
        out.println ""
        out.println ""
    }
//    out.println "    def stepInstanceId: Rep[Int] = column[Int]("stepInstanceId", O.PrimaryKey, O.AutoInc)"
//    out.println ""
//    out.println "    def stepId: Rep[Int] = column[Int]("stepId")"
//    out.println ""

    out.println "${indentSpaces}${indentSpaces}def * : ProvenShape[${className}Model] = ("
//    out.println "${indentSpaces}${indentSpaces}${indentSpaces}stepInstanceId.?,"
//    out.println "${indentSpaces}${indentSpaces}${indentSpaces}stepId,"
    sep = ""
    fields.each() {
        out.print sep
        sep = ",\n"
//        def varType = "${it.nullable ? 'Option[' + it.type + ']' : it.type}"
        out.print "${indentSpaces}${indentSpaces}${indentSpaces}${it.name}"
        if (it.key && !it.nullable) out.print ".?"
//        if (it.annos != "") out.print " // ${it.annos}"
    }
//    out.println "          ) <> ( { tuple: ("
//    out.println "            Option[Int], // stepInstanceId"
//    out.println "              Int, // stepId"
//    out.println "              Int, // processInstanceId"
//    out.println "              Option[String], // startedAt"
//    out.println "              Option[String], // createdAt"
    out.println ""
    out.println "${indentSpaces}${indentSpaces}) <> ( { tuple: ("
    sep = ""
    suffix = ""
    extraPad = ""
    fields.each() {
        out.print sep + suffix
        sep = ","
        varType = "${it.nullable || it.key ? 'Option[' + it.type + ']' : it.type}"
        out.print extraPad
        out.print "${indentSpaces}${indentSpaces}${indentSpaces}${varType}"
        suffix = " // ${it.name}\n"
        extraPad = "${indentSpaces}"
//        if (it.key) out.print ".?"
//        if (it.annos != "") out.print " // ${it.annos}"
    }
    out.print suffix

//    out.println "      ) =>"
//    out.println "      StepInstanceModel("
//    out.println "        stepInstanceId = tuple._1,"
//    out.println "        stepId = tuple._2,"
    out.println "${indentSpaces}${indentSpaces}${indentSpaces}) =>"
    out.println "${indentSpaces}${indentSpaces}${indentSpaces}${className}Model("
    sep = ""
    suffix = ""
    tuple = 1
    fields.each() {
        out.print sep + suffix
        sep = ","
        varType = "${it.nullable || it.key ? 'Option[' + it.type + ']' : it.type}"
        out.print "${indentSpaces}${indentSpaces}${indentSpaces}${indentSpaces}${it.name} = tuple._${tuple++}"
        suffix = "\n"
//        if (it.key) out.print ".?"
//        if (it.annos != "") out.print " // ${it.annos}"
    }
    out.print suffix

    out.println "${indentSpaces}${indentSpaces}${indentSpaces})"
    out.println "${indentSpaces}${indentSpaces}}, {"
    out.println "${indentSpaces}${indentSpaces}${indentSpaces}ps: ${className}Model =>"
    out.println "${indentSpaces}${indentSpaces}${indentSpaces}${indentSpaces}Some(("
//    out.println "          ps.stepInstanceId,"
//    out.println "          ps.stepId,"
    sep = ""
    suffix = ""
    tuple = 1
    fields.each() {
        out.print sep + suffix
        sep = ","
        varType = "${it.nullable || it.key ? 'Option[' + it.type + ']' : it.type}"
        out.print "${indentSpaces}${indentSpaces}${indentSpaces}${indentSpaces}${indentSpaces}ps.${it.name}"
        suffix = "\n"
//        if (it.key) out.print ".?"
//        if (it.annos != "") out.print " // ${it.annos}"
    }
    out.print suffix
    out.println "${indentSpaces}${indentSpaces}${indentSpaces}${indentSpaces}))"
    out.println "${indentSpaces}${indentSpaces}})"
    out.println "${indentSpaces}}"
    out.println ""
    out.println "${indentSpaces}val ${lowerFirst(toPlural(className))}: TableQuery[${className}] = TableQuery[${className}] // Query Object"
    out.println "}"
}
