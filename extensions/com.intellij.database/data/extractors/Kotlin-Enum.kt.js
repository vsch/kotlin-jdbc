/*
   Data extractor of table data to Kotlin Enum based on values

   First non-numeric column is considered the name of the enum values, the rest will be used as properties of the enum value

   Transposed status is ignored
*/

var packageName = "";    // package used for generated class files
var enumNameSuffix = "";  // appended to class file name

function eachWithIdx(iterable, f) {
    var i = iterable.iterator();
    var idx = 0;
    while (i.hasNext()) f(i.next(), idx++);
}

function mapEach(iterable, f) {
    var vs = [];
    eachWithIdx(iterable, function (i) {
        vs.push(f(i));
    });
    return vs;
}

function escape(str) {
    str = str.replaceAll("\t|\b|\\f", "");
    // str = com.intellij.openapi.util.text.StringUtil.escapeXml(str);
    str = str.replaceAll("\\r|\\n|\\r\\n", "");
    str = str.replaceAll("([\\[\\]\\|])", "\\$1");
    return str;
}

var NEWLINE = "\n";

function output() {
    for (var i = 0; i < arguments.length; i++) {
        OUT.append(arguments[i].toString());
    }
}

function outputln() {
    for (var i = 0; i < arguments.length; i++) {
        OUT.append(arguments[i].toString());
    }
    OUT.append("\n");
}

function outputRow(items) {
    output("| ");
    for (var i = 0; i < items.length; i++) output(escape(items[i]), " |");
    output("", NEWLINE);
}

function isObjectLike(param) {
    return !!param && typeof param === "object";
}

function isNumeric(arg) {
    return !!arg.match(/^(?:[+-]?[0-9]+(?:\.[0-9]*)?|[+-]?[0-9]*\.[0-9]+)(?:E[+-]?[0-9]+)?$/);
}

/**
 *
 * @param arr {Array}
 * @returns {boolean}
 */
function allStrings(arr) {
    var iMax = arr.length;
    for (i = 0; i < iMax; i++) {
        if (!arr[i].toString().match(/[a-zA-Z_][a-zA-Z_0-9]*/)) {
            return false;
        }
    }
    return true;
    // return arr.every(function (value) {
    //     return value.match(/[a-zA-Z_][a-zA-Z_0-9]*/);
    // });
}

// com.intellij.openapi.util.text.StringUtil.escapeXml(str)
function javaName(str, capitalize, pluralize, dropLastId) {
    var s = [];
    var spl = com.intellij.psi.codeStyle.NameUtil.splitNameIntoWords(str);

    var iMax = spl.length;
    for (i = 0; i < iMax; i++) {
        var part = spl[i].toString();
        if (!(dropLastId && i + 1 === iMax && iMax > 1 && part.toLowerCase() === "id")) {
            s.push(part);
        }
    }

    s = s.map(function (value, i) {
        var part = value;
        if (pluralize && i + 1 === s.length) {
            part = com.intellij.openapi.util.text.StringUtil.pluralize(part) || part;
        }

        return part.substr(0, 1).toUpperCase() + part.substring(1).toLowerCase();
    });

    var name = s.map(function (it) {
        return it.replace(/[^a-zA-Z0-9_$]+/, "_");
    }).join("");

    return capitalize || name.length === 1 ? name : name.substr(0, 1).toLowerCase() + name.substring(1)
}

function snakeName(str, capitalize, pluralize, dropLastId) {
    var s = [];
    var spl = com.intellij.psi.codeStyle.NameUtil.splitNameIntoWords(str);

    var iMax = spl.length;
    for (i = 0; i < iMax; i++) {
        var part = spl[i].toString();
        if (!(dropLastId && i + 1 === iMax && iMax > 1 && part.toLowerCase() === "id")) {
            s.push(part);
        }
    }

    s = s.map(function (value, i) {
        var part = value;
        if (pluralize && i + 1 === s.length) {
            part = com.intellij.openapi.util.text.StringUtil.pluralize(part) || part;
        }

        return part.substr(0, 1).toUpperCase() + part.substring(1).toLowerCase();
    });

    return s.map(function (it) {
        return it.replace(/[^a-zA-Z0-9$]+/, "_");
    }).join("_");
}

function values(obj) {
    var key;
    var values = [];

    for (key in obj) {
        if (obj.hasOwnProperty(key)) {
            values.push(obj[key]);
        }
    }
    return values;
}

function keys(obj) {
    var key;
    var keys = [];

    for (key in obj) {
        if (obj.hasOwnProperty(key)) {
            keys.push(key);
        }
    }
    return keys;
}

var rows = [];
var columnNames;
var enumValueNames = [];
var enumNames = [];
var enumNameColumns = [];
var enumNamesColumn = -1;
var columns = [];
var enumName = "";

columnNames = mapEach(COLUMNS, function (col) {
    return col.name();
});

eachWithIdx(ROWS, function (row) {
    rows.push(mapEach(COLUMNS, function (col) {
        return FORMATTER.format(row, col);
    }));
});

columns = rows[0].map(function () {
    return [];
});

// need columns
rows.forEach(function (row, rowIndex) {
    row.forEach(function (value, columnIndex) {
        columns[columnIndex].push(value);
    })
});

// name taken from the first column, less id if it is not just id
enumName = javaName(columnNames[0], true, true, true);
if (enumName === "") {
    enumName = "TestEnum";
}

// find first non-numeric column for enum names
var iMax = columns.length;
var i;
for (i = 0; i < iMax; i++) {
    var column = columns[i];
    if (allStrings(column)) {
        enumNamesColumn = i;
        enumValueNames = [];
        var jMax = column.length;
        var j;
        for (j = 0; j < jMax; j++) {
            enumValueNames.push(snakeName(column[j], false, false, false).toUpperCase());
        }

        break;
    }
}

// use default enum value names
if (enumNamesColumn < 0) {
    enumNamesColumn = 0;
    var valueName = snakeName(columnNames[0], false, false, true).toUpperCase();
    enumValueNames = columns[0].map(function (value) {
        return valueName + "_" + value;
    });
}

enumNameColumns = {};
enumNames = columnNames
    .map(function (value, index) {
        var name = javaName(value, false, false, false);

        if (index === 0 && javaName(value, true, true, true).toLowerCase() === enumName.toLowerCase()) {
            name = "id";
        }
        enumNameColumns[name] = index;
        return name;
    });

enumName = enumName + enumNameSuffix;
var columnTypes = enumNames.map(function (value) {
    if (allStrings(columns[enumNameColumns[value]])) {
        return "String";
    } else {
        return "Int";
    }
});

/**
 *
 * @param callback      (name, type, colIndex) return
 * @param prefix        will be output iff enumNames is not empty and have output
 * @param delimiter     will be used between outputs
 * @param suffix        will be output iff enumNames is not empty and have output
 *
 *
 */
function forAllEnumParams(callback, prefix, delimiter, suffix) {
    var sep = prefix;
    var hadOutput = false;
    enumNames.forEach(function (value) {
        var enumNameColumn = enumNameColumns[value];
        var type = columnTypes[enumNameColumn];
        var out = callback(value, type, enumNameColumn);
        if (out !== undefined && out !== null) {
            output(sep);
            sep = delimiter;
            output(out);
            hadOutput = true;
        }
    });

    if (hadOutput && suffix !== undefined && suffix !== null) {
        output(suffix);
    }
}

// Start of output
// may need constructor
if (packageName) {
    outputln("package " + packageName);
    outputln("");
}

output("enum class " + enumName);

forAllEnumParams(function (name, type, isId, colIndex) {
    return "val " + name + ": " + type;
}, "(", ", ", ")");

outputln(" {");

var sep = "";
enumValueNames.forEach(function (value, enumValueIndex) {
    output(sep);
    sep = ",\n";
    output("  ", value);

    forAllEnumParams(function (name, type, colIndex) {
        var columnElement = columns[colIndex][enumValueIndex];
        return type === "String" ? '"' + columnElement.replace(/\\/g, "\\\\").replace(/"/g, "\\\"") + '"' : columnElement;
    }, "(", ", ", ")");
});
outputln(";");

forAllEnumParams(function (name, type, colIndex) {
    return ["    fun ", javaName(columnNames[colIndex]), "(", name, ": ", type, "): ", enumName, "? = values().find { it.", name, " == ", name, " }"].join("");
}, "\n  companion object {\n", "\n", "\n  }\n");

outputln("}");

