/*
   Data extractor of table data to JavaScript Enum based on values using npm 'enumerated-type' package

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
        OUT.append(arguments[i]);
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

// com.intellij.openapi.util.text.StringUtil.escapeXml(str)
function displayName(str, pluralize, dropLastId) {
    if (/^[A-Z_]+$/.test(str)) {
        return str;
    } else {
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
            return it.replace(/[^a-zA-Z0-9_$]+/, " ");
        }).join(" ").replace(/\\s+/, " ");
    }
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
            enumValueNames.push(javaName(column[j], false, false, false));
        }

        break;
    }
}

// use default enum value names
if (enumNamesColumn < 0) {
    enumNamesColumn = 0;
    enumValueNames = columns[0].map(function (value) {
        return columnNames[0] + "_" + value;
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
            if (sep !== undefined) {
                output(sep);
            }
            sep = delimiter;
            output(out);
            hadOutput = true;
        }
    });

    if (hadOutput && suffix !== undefined && suffix !== null) {
        output(suffix);
    }
}

/**
 * @param callback      (name, value, index) return
 * @param prefix        will be output iff enumNames is not empty and have output
 * @param delimiter     will be used between outputs
 * @param suffix        will be output iff enumNames is not empty and have output
 */
function forAllEnumValues(callback, prefix, delimiter, suffix) {
    var sep = prefix;
    var hadOutput = false;
    enumValueNames.forEach(function (value, index) {
        if (sep !== undefined) {
            output(sep);
        }

        sep = delimiter;
        var enumValue = columns[0][index];
        var out = callback(value, enumValue, index);
        if (out !== undefined && out !== null) {
            if (sep !== undefined) {
                output(sep);
            }
            sep = delimiter;
            output(out);
            hadOutput = true;
        }
    });

    if (hadOutput && suffix !== undefined && suffix !== null) {
        output(suffix);
    }
}

/**
 * @param callback      (name, value, index) return
 * @param prefix        will be output iff enumNames is not empty and have output
 * @param delimiter     will be used between outputs
 * @param suffix        will be output iff enumNames is not empty and have output
 */
function forAllNamedEnumValues(callback, prefix, delimiter, suffix) {
    if (enumNamesColumn > 0) {
        forAllEnumValues(callback, prefix, delimiter, suffix);
    }
}

var sep = "";

// Start of output
outputln("import Enum from 'enumerated-type';");
outputln();
var idPrefix = (snakeName(columnNames[0], false, false, true) + "_").toUpperCase();
var enumValueVar = javaName(columnNames[0], false, false, true);
var enumIdVar = javaName(columnNames[0]);

/*
forAllNamedEnumValues(function (name, value, index) {
    return ["export const ", idPrefix, snakeName(name, false, false, true).toUpperCase(), "_ID = ", value, ";\n"].join("");
}, "", "", "\n");
*/

outputln("// NOTE: this definition is used for WebStorm Code Completions to work");
outputln("const ", enumName, "Value = {");

output("    value: {", enumIdVar, ": 0");
sep = ", ";
forAllEnumParams(function (nameAs, type, colIndex) {
    if (colIndex) {
        output(sep, javaName(nameAs, false, false, false), ": ");
        var elementValue = type === "String" ? '""' : 0;
        output(elementValue);
    }
});
outputln(sep, "_displayName: \"\"},\n");

outputln("    get ", enumIdVar, "() { return this.value.", enumIdVar, "; },");
forAllEnumParams(function (nameAs, type, colIndex) {
    if (colIndex) {
        outputln("    get ", javaName(nameAs, false, false, false), "() { return this.value.", javaName(nameAs, false, false, false), "; },");
    }
});
outputln();

// output test functions for each named value
forAllNamedEnumValues(function (name, value, index) {
    return ["    get is", javaName(name, true, false, false), "() { return this === ", enumName, ".", javaName(name, false, false, false), "; },\n"].join("");
}, "", "", "");

outputln("};");

outputln();
outputln("// NOTE: this definition is used for WebStorm Code Completions to work");
outputln("let ", enumName, " = {");
forAllNamedEnumValues(function (name, value, index) {
    return ["    ", name, ": ", enumName, "Value, // ", value, "\n"].join("");
}, "", "", "");
outputln();
outputln("    value(arg, defaultValue = undefined) { return ", enumName, "Value; }, // return an item with ", enumIdVar, " matching arg or defaultValue");
outputln("    ", enumIdVar, "(arg) { return ", enumName, "Value.", enumIdVar, "; }, // return the arg if it matches an item's ", enumIdVar, " or ", enumValueNames[0], ".", enumIdVar);
outputln("    get dropdownChoices() { return [{value: 0, label: \"label\"}]; }, // return dropdownChoices array");
outputln("    dropdownChoicesExcluding() { return [{value: 0, label: \"label\"}]; }, // return dropdownChoices array excluding ones for items passed as arguments to the function");
outputln("};");
outputln();

outputln("// this definition is used for actual definition");
outputln("let _", enumName, " = {");

sep = ", ";
forAllNamedEnumValues(function (name, value, index) {
    var arr = ["    ", name, ": {"];

    var enumDisplayValue;

    forAllEnumParams(function (nameAs, type, colIndex) {
        if (!colIndex) {
            arr.push(enumIdVar, ": ");
        } else {
            arr.push(sep, javaName(nameAs, false, false, false), ": ");
        }
        var columnElement = columns[colIndex][index];

        if (nameAs === enumValueVar) {
            enumDisplayValue = displayName(columnElement, false, false);
        }

        var elementValue = type === "String" ? '"' + columnElement.replace(/\\/g, "\\\\").replace(/"/g, "\\\"") + '"' : columnElement;
        arr.push(elementValue);
    }, "", "", "");

    arr.push(sep, "_displayName: ", '"' + enumDisplayValue.replace(/\\/g, "\\\\").replace(/"/g, "\\\"") + '"');
    arr.push("},\n");
    return arr.join("");
});

outputln("};");

outputln();
outputln(enumName, " = new Enum(\"", enumName, "\", _", enumName, ", ", enumName, "Value, \"", enumIdVar, "\", \"_displayName\");");
outputln();
outputln("export default ", enumName, ";");
outputln();

outputln("/**");
outputln(" * Sample function with switch for handling all enum values and generating exceptions,");
outputln(" * use the copy/paste Luke, use the copy/paste");
outputln(" *");
outputln(" * @param ", enumIdVar, " {*} enum value or value which can be converted to enum value");
outputln(" */");
outputln("function dummy(", enumIdVar, ") {");
outputln("    const ", enumValueVar, " = ", enumName, ".value(", enumIdVar, ");");
outputln("    switch (", enumValueVar, ") {");

forAllNamedEnumValues(function (name, value, index) {
    return ["        case ", enumName, ".", name, ":\n"].join("");
}, "", "", "");

outputln("            break;");
outputln("");
outputln("        default:");
outputln("            if (", enumValueVar, ") {");
outputln("                // throw an error instead of silently doing nothing if more types are added in the future");
outputln("                throw `IllegalStateException unhandled ", enumName, ".${", enumValueVar, ".name} value ${", enumValueVar, ".value}`;");
outputln("            } else {");
outputln("                // comment out if non-enum values are allowed and ignored");
outputln("                throw `IllegalArgumentException unknown ", enumName, " value ${", enumIdVar, "}`;");
outputln("            }");
outputln("    }");
outputln("}");
