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
packageName = "com.sample" // package used for generated class files
classFileNameSuffix = "Model" // appended to class file name

// KLUDGE: the DasTable and columns does implement a way to get whether column is nullable, has default or is computed
forceNullable = (~/^(?:createdAt|created_at|updatedAt|updated_at)$/) // regex for column names which are forced to nullable Kotlin type
forceAuto = (~/^(?:createdAt|created_at|updatedAt|updated_at)$/) // column names marked auto generated
forceTimestampString = (~/^(?:createdAt|created_at|updatedAt|updated_at)$/) // column names if timestamp type will be changed to String

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
    def className = javaName(singularize(table.getName()), true)
    def fields = calcFields(table)
    new File(dir, className + "${classFileNameSuffix}.scala").withPrintWriter { out -> generate(out, table.getName(), className, fields) }
}

static def rightPad(text, len) {
    def padded = text
    def pad = len - text.length()
    while (pad-- > 0) padded += " "
    return padded
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
    out.println ""
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
        out.print "  * @param "
        if (it.annos != "") out.println "  ${it.annos}"
        out.print "${rightPad(it.name, len)} ${it.nullable ? 'Option[' + it.type + ']':it.type}"
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
        out.print "  ${it.name}: ${it.nullable ? 'Option[' + it.type + '] = None':it.type}"
        sep = ",\n"
        if (it.annos != "") out.print " // ${it.annos}"
    }

    out.println "\n)"
    out.println ""

    out.println "object ${className}Model {"
    out.println "  implicit val ${lowerFirst(className)}Format: OFormat[${className}Model] = Json.format[${className}Model]"
    out.println "}"
    out.println ""

    out.println "trait ${className}Component {"
    out.println "  self: HasDatabaseConfigProvider[JdbcProfile] =>"
    out.println ""
    out.println "  import profile.api._"
    out.println ""
    out.println "  class ${className}(tag: Tag) extends Table[${className}Model](tag, \"${tableName}\") {"
    sep = ""
    fields.each() {
        out.print sep
        varType = "${it.nullable ? 'Option[' + it.type + ']' : it.type}"
        out.print "    def ${it.name}: Rep[${varType}] = column[${varType}](\"${it.name}\""
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


    out.println "    def * : ProvenShape[${className}Model] = ("
//    out.println "      stepInstanceId.?,"
//    out.println "      stepId,"
    sep = ""
    fields.each() {
        out.print sep
        sep = ",\n"
//        def varType = "${it.nullable ? 'Option[' + it.type + ']' : it.type}"
        out.print "      ${it.name}"
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
    out.println "    ) <> ( { tuple: ("
    sep = ""
    suffix = ""
    extraPad = ""
    fields.each() {
        out.print sep + suffix
        sep = ","
        varType = "${it.nullable || it.key ? 'Option[' + it.type + ']' : it.type}"
        out.print extraPad
        out.print "      ${varType}"
        suffix = " // ${it.name}\n"
        extraPad = "  "
//        if (it.key) out.print ".?"
//        if (it.annos != "") out.print " // ${it.annos}"
    }
    out.print suffix

//    out.println "      ) =>"
//    out.println "      StepInstanceModel("
//    out.println "        stepInstanceId = tuple._1,"
//    out.println "        stepId = tuple._2,"
    out.println "      ) =>"
    out.println "      ${className}Model("
    sep = ""
    suffix = ""
    tuple = 1
    fields.each() {
        out.print sep + suffix
        sep = ","
        varType = "${it.nullable || it.key ? 'Option[' + it.type + ']' : it.type}"
        out.print "        ${it.name} = tuple._${tuple++}"
        suffix = "\n"
//        if (it.key) out.print ".?"
//        if (it.annos != "") out.print " // ${it.annos}"
    }
    out.print suffix

    out.println "      )"
    out.println "    }, {"
    out.println "      ps: ${className}Model =>"
    out.println "        Some(("
//    out.println "          ps.stepInstanceId,"
//    out.println "          ps.stepId,"
    sep = ""
    suffix = ""
    tuple = 1
    fields.each() {
        out.print sep + suffix
        sep = ","
        varType = "${it.nullable || it.key ? 'Option[' + it.type + ']' : it.type}"
        out.print "          ps.${it.name}"
        suffix = "\n"
//        if (it.key) out.print ".?"
//        if (it.annos != "") out.print " // ${it.annos}"
    }
    out.print suffix
    out.println "        ))"
    out.println "    })"
    out.println "  }"
    out.println ""
    out.println "  val ${lowerFirst(pluralize(className))}: TableQuery[${className}] = TableQuery[${className}] // Query Object"
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
                           type    : forceTimestampString.matcher(colName) && colType in ['Timestamp'/*, 'Date', 'Datetime', 'Time'*/] ? "String": colType,
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

def singularize(str) {
    def s = com.intellij.psi.codeStyle.NameUtil.splitNameIntoWords(str).collect { it }
    s[-1] = com.intellij.openapi.util.text.StringUtil.unpluralize(s[-1])
    return s.join("")
}

def pluralize(str) {
    def s = com.intellij.psi.codeStyle.NameUtil.splitNameIntoWords(str).collect { it }
    s[-1] = com.intellij.openapi.util.text.StringUtil.pluralize(s[-1])
    return s.join("")
}

def lowerFirst(str) {
    return str.length() > 0 ? Case.LOWER.apply(str[0]) + str[1..-1] : str
}

def javaName(str, capitalize) {
    def s = com.intellij.psi.codeStyle.NameUtil.splitNameIntoWords(str)
            .collect { Case.LOWER.apply(it).capitalize() }
            .join("")
            .replaceAll(/[^\p{javaJavaIdentifierPart}[_]]/, "_")
    capitalize || s.length() == 1 ? s : Case.LOWER.apply(s[0]) + s[1..-1]
}
