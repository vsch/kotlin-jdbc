import com.intellij.database.model.DasConstraint
import com.intellij.database.model.DasTable
import com.intellij.database.model.ObjectKind
import com.intellij.database.util.Case
import com.intellij.database.util.DasUtil

/*
 * Available context bindings:
 *   SELECTION   Iterable<DasObject>
 *   PROJECT     project
 *   FILES       files helper
 */
packageName = "com.sample"      // package used for generated class files
classFileNameSuffix = "Model"   // appended to class file name

// KLUDGE: the DasTable and columns does not implement a way to get whether column is nullable, has default or is computed
forceNullable = (~/^(?:createdAt|created_at|updatedAt|updated_at)$/) // regex for column names which are forced to nullable Kotlin type
forceAuto = (~/^(?:createdAt|created_at|updatedAt|updated_at)$/) // column names marked auto generated
snakeCaseTables = false  // if true convert snake_case table names to Pascal case, else leave as is

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

FILES.chooseDirectoryAndSave("Choose directory", "Choose where to store generated files") { dir ->
    SELECTION.filter { it instanceof DasTable && it.getKind() == ObjectKind.TABLE }.each { generate(it, dir) }
}

def generate(table, dir) {
    def className = snakeCaseTables ? javaName(singularize(table.getName()), true) : singularize(table.getName())
    def fields = calcFields(table)
    new File(dir, className + "${classFileNameSuffix}.kt").withPrintWriter { out -> generate(out, table.getName(), className, fields) }
}

def generate(out, tableName, className, fields) {
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
    out.println "import com.vladsch.kotlin.jdbc.Row"
    if (timestampCount > 0) out.println "import java.sql.Timestamp"
    if (dateCount > 0) out.println "import java.sql.Date"
    if (timeCount > 0) out.println "import java.sql.Time"
    out.println "import javax.json.JsonObject"
    out.println ""
    out.println "data class $className ("

    def sep = "";
    fields.each() {
        out.print sep
        sep = ",\n";
        if (it.annos != "") out.println "  ${it.annos}"
        out.print "  val ${it.name}: ${it.type}"
        if (it.nullable) out.print "?"
    }
    out.println "\n)"
    out.println ""

    // model class
    out.println "@Suppress(\"MemberVisibilityCanBePrivate\")"
    out.println "class ${className}Model() : Model<${className}Model>(tableName, dbCase = ${dbCase}) {"
    def maxWidth = 0
    def lines = []
    fields.each() {
        def line = ""
        if (it.annos != "") line += "  ${it.annos}"
        line += "  var ${it.name}: ${it.type}"
        if (it.nullable) line += "?"
        line += " by model"
        if (it.auto) line += ".auto"
        if (it.key) line += ".key"
        if (maxWidth < line.length()) maxWidth = line.length()
        lines.add(line)
    }

    maxWidth += 7 - (maxWidth % 4)

    def i = 0
    lines.each() { it ->
        out.print it
        out.print "                                                                                                              ".substring(0, maxWidth - it.length())
        out.println "// ${fields[i].column} ${fields[i].spec}"
        i++
    }

    // data copy constructor
    out.println ""
    out.println "  constructor(other: ${className}) : this() {"
    fields.each() {
        out.println "    ${it.name} = other.${it.name}"
    }
    out.println "  }"
    out.println ""

    // data factory
    out.println "  fun toData() = ${className}("
    sep = "";
    fields.each() {
        out.print sep
        sep = ",\n";
        out.print "    ${it.name}"
    }
    out.println "\n  )"
    out.println ""

    out.println "  companion object {"
    out.println "    const val tableName = \"${tableName}\""
    out.println ""
    out.println "    val toModel: (Row) -> ${className}Model = { row ->"
    out.println "      ${className}Model().load(row)"
    out.println "    }"
    out.println ""
    out.println "    val toData: (Row) -> ${className} = { row ->"
    out.println "      ${className}Model().load(row).toData()"
    out.println "    }"
    out.println ""
    out.println "    val toJson: (Row) -> JsonObject = { row ->"
    out.println "      ${className}Model().load(row).toJson()"
    out.println "    }"
    out.println "  }"
    out.println "}"
}

def calcFields(table) {
    def constraints = ((DasTable) table).getDbChildren(DasConstraint.class, ObjectKind.CHECK);
    def colIndex = 0
    DasUtil.getColumns(((DasTable) table)).reduce([]) { fields, col ->
        def spec = Case.LOWER.apply(col.getDataType().getSpecification())
        def typeStr = (String) typeMapping.find { p, t -> p.matcher(spec).find() }.value
        def colName = (String) col.getName()
        def colNameLower = (String) Case.LOWER.apply(colName)
        colIndex++
        def colType = downsizeLongIdToInt && typeStr == "Long" && DasUtil.isPrimary(col) || DasUtil.isForeign(col) && colNameLower.endsWith("id") ? "Int" : typeStr
        def javaColName = (String) javaName(colName, false)
        if (typeStr == "TinyInt") {
            if (forceBooleanTinyInt && javaColName.matches(forceBooleanTinyInt)) {
                colType = "Boolean"
            } else {
                colType = "Int"
            }
        }
        fields += [[
                           name    : javaColName,
                           column  : colName,
                           type    : colType,
                           col     : col,
                           spec    : spec,
//                           constraints : constraints.reduce("") { all, constraint ->
//                               all += "[ name: ${constraint.name}, " + "kind: ${constraint.getKind()}," + "]"
//                           },
                           nullable: DasUtil.isAutoGenerated(col) || forceNullable.matcher(colName),
                           key     : DasUtil.isPrimary(col),
                           auto    : DasUtil.isAutoGenerated(col) || forceAuto.matcher(colName),
                           annos   : ""]]
    }
}

def singularize(String str) {
    def s = com.intellij.psi.codeStyle.NameUtil.splitNameIntoWords(str).collect { it }
    def singleLast = com.intellij.openapi.util.text.StringUtil.unpluralize(s[-1])
    return str.substring(0, str.length() - s[-1].length()) + singleLast
}

def pluralize(String str) {
    def s = com.intellij.psi.codeStyle.NameUtil.splitNameIntoWords(str).collect { it }
    def pluralLast = com.intellij.openapi.util.text.StringUtil.pluralize(s[-1])
    return str.substring(0, str.length() - s[-1].length()) + pluralLast
}

def javaName(str, capitalize) {
    def s = com.intellij.psi.codeStyle.NameUtil.splitNameIntoWords(str)
            .collect { Case.LOWER.apply(it).capitalize() }
            .join("")
            .replaceAll(/[^\p{javaJavaIdentifierPart}[_]]/, "_")
    capitalize || s.length() == 1 ? s : Case.LOWER.apply(s[0]) + s[1..-1]
}
