package com.vladsch.kotlin.jdbc

import java.sql.Savepoint

interface Transaction: Session {
    fun commit()
    fun begin()
    fun rollback()
    fun setSavepoint(): Savepoint
    fun setSavepoint(name: String): Savepoint
    fun rollback(savepoint: Savepoint)
    fun releaseSavepoint(savepoint: Savepoint)
}
