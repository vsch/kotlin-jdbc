package com.vladsch.kotlin.jdbc

interface DbEntityFixer {
    object NULL : DbEntityFixer {
        override fun cleanScript(createScript: String): String {
            return createScript
        }
    }

    fun cleanScript(createScript: String): String
}
