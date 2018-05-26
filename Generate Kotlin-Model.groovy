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

packageName = "com.sample;"
typeMapping = [
        (~/(?i)tinyint\(1\)/)             : "Boolean",
        (~/(?i)bigint/)                   : "Long",
        (~/(?i)int/)                      : "Int",
        (~/(?i)float/)                    : "Float",
        (~/(?i)double|decimal|real/)      : "Double",
        (~/(?i)datetime|timestamp/)       : "java.sql.Timestamp",
        (~/(?i)date/)                     : "java.sql.Date",
        (~/(?i)time/)                     : "java.sql.Time",
        (~/(?i)/)                         : "String"
]

forceNullable = (~/^(?:createdAt|created_at|updatedAt|updated_at)/)
forceAuto = (~/^(?:createdAt|created_at|updatedAt|updated_at)/)

FILES.chooseDirectoryAndSave("Choose directory", "Choose where to store generated files") { dir ->
    SELECTION.filter { it instanceof DasTable && it.getKind() == ObjectKind.TABLE }.each { generate(it, dir) }
}

def generate(table, dir) {
    def className = javaName(table.getName(), true)
    def fields = calcFields(table)
    new File(dir, className + ".kt").withPrintWriter { out -> generate(out, table.getName(), className, fields) }
}

def generate(out, tableName, className, fields) {
    out.println "package $packageName"
    out.println ""
    out.println "import com.vladsch.kotlin.jdbc.*"
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

    def dbCase = true
    fields.each() {
        if (it.name != it.column) dbCase = false
    }

    // model class
    out.println "@Suppress(\"MemberVisibilityCanBePrivate\")"
    out.println "class ${className}Model() : Model<${className}Model>(tableName, dbCase = ${dbCase}) {"
    def maxWidth = 0
    def lines = []
    fields.each() {
        def line = ""
        if (it.annos != "") line += "  ${it.annos}"
        line +=  "  var ${it.name}: ${it.type}"
        if (it.nullable) line +=  "?"
        line +=  " by model"
        if (it.auto) line += ".auto"
        if (DasUtil.isPrimary(it.col)) line +=  ".key"
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
    out.println "  constructor(other:${className}) : this() {"
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
    def constraints = ((DasTable)table).getDbChildren(DasConstraint.class, ObjectKind.CHECK);
    DasUtil.getColumns(((DasTable)table)).reduce([]) { fields, col ->
        def spec = Case.LOWER.apply(col.getDataType().getSpecification())
        def typeStr = (String)typeMapping.find { p, t -> p.matcher(spec).find() }.value
        def colName = (String)col.getName()
        def colNameLower = (String)Case.LOWER.apply(colName)
        fields += [[
                           name : javaName(colName, false),
                           column: colName,
                           type : typeStr == "Long" && DasUtil.isPrimary(col) || DasUtil.isForeign(col) && colNameLower.endsWith("id") ? "Int" : typeStr,
                           col  : col,
                           spec : spec,
//                           constraints : constraints.reduce("") { all, constraint ->
//                               all += "[ name: ${constraint.name}, " + "kind: ${constraint.getKind()}," + "]"
//                           },
                           nullable : DasUtil.isAutoGenerated(col) || DasUtil.isPrimary(col) || forceNullable.matcher(colName),
                           auto : DasUtil.isAutoGenerated(col) || DasUtil.isPrimary(col) || forceAuto.matcher(colName),
                           annos : ""]]
    }
}

def javaName(str, capitalize) {
    def s = com.intellij.psi.codeStyle.NameUtil.splitNameIntoWords(str)
            .collect { Case.LOWER.apply(it).capitalize() }
            .join("")
            .replaceAll(/[^\p{javaJavaIdentifierPart}[_]]/, "_")
    capitalize || s.length() == 1 ? s : Case.LOWER.apply(s[0]) + s[1..-1]
}
