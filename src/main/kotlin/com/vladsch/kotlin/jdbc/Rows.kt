package com.vladsch.kotlin.jdbc

import java.sql.ResultSet

class Rows(rs: ResultSet) : Row(rs), Sequence<Row> {
    private class RowIterator(val row:Rows):Iterator<Row> {
        override fun hasNext(): Boolean {
            return !(row.rs.isClosed || row.rs.isLast || row.rs.isAfterLast)
        }

        override fun next(): Row {
            return row.next();
        }
    }

    override fun iterator(): Iterator<Row> {
        return RowIterator(this);
    }

    fun hasNext(): Boolean {
        return !(rs.isClosed || rs.isLast || rs.isAfterLast)
    }

    fun next(): Rows {
        rs.next()
        return this;
    }
}
