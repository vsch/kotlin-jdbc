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
DEBUG = false                   // if true output debug trace to debug.log
classFileNameSuffix = "Model"   // appended to class file name
snakeCaseTables = false         // if true convert snake_case table names to Pascal case, else leave as is
sp = "    "                     // spaces for each indent level
fileExtension = ".kt"

// column names marked as boolean when tinyint, only needed if using jdbc introspection which does not report actual declared type so all tinyint are tinyint(3)
//forceBooleanTinyInt = (~/^(?:deleted|checkedStatus|checked_status|optionState|option_state)$/)
forceBooleanTinyInt = ""

downsizeLongIdToInt = true // if true changes id columns which would be declared Long to Int, change this to false to leave them as Long

typeMapping = [
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
    out.println "import com.vladsch.kotlin.jdbc.Model"
    out.println "import com.vladsch.kotlin.jdbc.Session"
//    out.println "import com.vladsch.kotlin.jdbc.ModelCompanion"
    if (timestampCount > 0) out.println "import java.sql.Timestamp"
    if (dateCount > 0) out.println "import java.sql.Date"
    if (timeCount > 0) out.println "import java.sql.Time"
//    out.println "import javax.json.JsonObject"
    out.println ""
    out.println "data class $className("

    def sep = "";
    fields.each() {
        out.print sep
        sep = ",\n";
        if (it.annos != "") out.println "${sp}${it.annos}"
        out.print "${sp}val ${it.name}: ${it.type}"
        if (it.nullable) out.print "?"
//        if (it.attrs != null) out.print(" // attrs: '${it.attrs}'")
//        if (it.default != null) out.print(" // default: '${it.default}'")
        if (it.suffix != null && it.suffix != "") out.print(" // suffix: '${it.suffix}'")
    }
    out.println "\n)"
    out.println ""

    // model class
    out.println "@Suppress(\"MemberVisibilityCanBePrivate\")"
    out.println "class ${className}Model(session: Session? = null, quote: String? = null) : Model<${className}Model, ${className}>(session, tableName, dbCase = ${dbCase}, quote = quote) {"
    def maxWidth = 0
    def lines = []

    fields.each() {
        def line = ""
        if (it.annos != "") line += "${sp}${it.annos}"
        line += "${sp}var ${it.name}: ${it.type}"
        if (it.nullable) line += "?"
        line += " by db"
        if (it.auto && it.key) line += ".autoKey"
        else if (it.auto) line += ".auto"
        else if (it.key) line += ".key"
        else if (it.default) line += ".default"

        if (maxWidth < line.length()) maxWidth = line.length()
        lines.add(line)
    }

    maxWidth += 7 - (maxWidth % 4)

    def i = 0
    lines.each() { it ->
        out.print rightPad(it, maxWidth)
        out.println "// ${fields[i].column} ${fields[i].spec}${fields[i].notNull ? " NOT NULL" : ""}${fields[i].autoInc ? " AUTO_INCREMENT" : ""}${defaultValue(fields[i].default)}${fields[i].suffix != null ? " ${fields[i].suffix}" : ""}"
        i++
    }

    // data copy constructor
    out.println ""
    out.println "${sp}constructor(other: ${className}, session: Session? = null, quote: String? = null) : this(session, quote) {"
    fields.each() {
        out.println "${sp}${sp}${it.name} = other.${it.name}"
    }
    out.println "${sp}${sp}snapshot()"
    out.println "${sp}}"
    out.println ""

    // copy factory
    out.println "${sp}override operator fun invoke() = ${className}Model(_session, _quote)"
    out.println ""

    // data factory
    out.println "${sp}override fun toData() = ${className}("
    sep = "";
    fields.each() {
        out.print sep
        sep = ",\n";
        out.print "${sp}${sp}${it.name}"
    }
    out.println "\n${sp})"
    out.println ""
    out.println "${sp}companion object {"
    out.println "${sp}${sp}const val tableName = \"${tableName}\""
//    out.println "${sp}${sp}override fun createModel(quote:String?): ${className}Model = ${className}Model(quote)"
//    out.println "${sp}${sp}override fun createData(model: ${className}Model): ${className} = model.toData()"
    out.println "${sp}}"
    out.println "}"
}
