package com.vladsch.kotlin.jdbc

internal interface InternalModelPropertyProvider<T>:ModelPropertyProvider<T> {
    val columnName:String?
}
