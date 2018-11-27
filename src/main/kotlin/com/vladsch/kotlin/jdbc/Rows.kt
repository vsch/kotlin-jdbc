package com.vladsch.kotlin.jdbc

import java.sql.ResultSet

class Rows(rs: ResultSet) : Row(rs), Sequence<Row> {
    private var movedNext = false

    private class RowIterator(val row:Rows):Iterator<Row> {

        override fun hasNext(): Boolean {
            return row.hasNext()
        }

        override fun next(): Row {
            return row.next();
        }
    }

    override fun iterator(): Iterator<Row> {
        return RowIterator(this);
    }

    fun hasNext(): Boolean {
        if (rs.isClosed || (rs.type != ResultSet.TYPE_FORWARD_ONLY && (rs.isLast || rs.isAfterLast))) return false
        if (movedNext) return true

        movedNext = rs.next()
        return movedNext
    }

    fun next(): Rows {
        if (movedNext) {
            movedNext = false
            return this
        }

        rs.next()
        return this;
    }
}
