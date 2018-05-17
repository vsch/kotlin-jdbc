package com.vladsch.kotlin.jdbc

import java.sql.SQLException

data class Connection(
    val underlying: java.sql.Connection,
    val driverName: String = "",
    val jtaCompatible: Boolean = false
) : java.sql.Connection by underlying {

    fun begin() {
        underlying.autoCommit = false
        if (!jtaCompatible) {
            underlying.isReadOnly = false
        }
    }

    override fun commit() {
        underlying.commit()
        underlying.autoCommit = true
    }

    override fun rollback() {
        underlying.rollback()
        try {
            underlying.autoCommit = true
        } catch (e: SQLException) {

        }
    }

    override fun close() {
        underlying.close()
    }
}
