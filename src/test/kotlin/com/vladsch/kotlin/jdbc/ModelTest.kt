package com.vladsch.kotlin.jdbc

//import kotlin.test.assertEquals
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import java.sql.DriverManager

fun session(): Session {
    val driverName = "org.h2.Driver"

    val connection = DriverManager.getConnection("jdbc:h2:mem:hello", "user", "pass")
    val session = object : SessionImpl(Connection(connection, driverName)) {
        override val identifierQuoteString: String
            get() = ""
    }
    return session
}

@Suppress("UNUSED_VARIABLE")
class ModelTest {

    @Rule
    @JvmField
    var thrown: ExpectedException = ExpectedException.none()

    class InvalidModelPublicAutoKey(session: Session? = session(), quote: String? = null) : Model<InvalidModelPublicAutoKey, InvalidModelPublicAutoKey.Data>(session, "tests", true, false, quote = quote) {
        data class Data(
            val processId: Long?,
            val title: String,
            val version: String,
            val unknown: String?,
            val createdAt: String?
        )

        var processId: Long? by db.autoKey
        var title: String by db
        var version: String by db
        var unknown: String? by db
        var createdAt: String? by db.auto; private set

        override fun invoke(): InvalidModelPublicAutoKey {
            return InvalidModelPublicAutoKey(_session, _quote)
        }

        override fun toData(): Data {
            return Data(processId, title, version, unknown, createdAt)
        }
    }

    class InvalidModelPublicAuto(session: Session? = session(), quote: String? = null) : Model<InvalidModelPublicAuto, InvalidModelPublicAuto.Data>(session, "tests", true, false, quote = quote) {
        data class Data(
            val processId: Long?,
            val title: String,
            val version: String,
            val unknown: String?,
            val createdAt: String?
        )

        var processId: Long? by db.autoKey; private set
        var title: String by db
        var version: String by db
        var unknown: String? by db
        var createdAt: String? by db.auto

        override fun invoke(): InvalidModelPublicAuto {
            return InvalidModelPublicAuto(_session, _quote)
        }

        override fun toData(): Data {
            return Data(processId, title, version, unknown, createdAt)
        }
    }

    class ValidModelPublicAuto(session: Session? = session(), quote: String? = null) : Model<ValidModelPublicAuto, ValidModelPublicAuto.Data>(session, sqlTable = "tests", dbCase = true, quote = quote) {
        data class Data(
            val processId: Long?,
            val title: String,
            val version: String,
            val unknown: String?,
            val createdAt: String?
        )

        var processId: Long? by db.autoKey
        var title: String by db
        var version: String by db
        var unknown: String? by db
        var createdAt: String? by db.auto

        override fun invoke(): ValidModelPublicAuto {
            return ValidModelPublicAuto(_session, _quote)
        }

        override fun toData(): Data {
            return Data(processId, title, version, unknown, createdAt)
        }
    }

    data class ValidData(
        val processId: Long?,
        val noSetter: String,
        val noSetter2: String,
        val title: String,
        val version: String,
        val unknown: String?,
        val createdAt: String?,
        val createdAt2: String?
    )

    class ValidModel(session: Session? = session(), quote: String? = null) : Model<ValidModel, ValidData>(session, tableName, true, false, quote) {

        var processId: Long? by db.key.auto; private set
        val noSetter: String by db.auto
        val noSetter2: String by db.autoKey
        var title: String by db
        var version: String by db
        var unknown: String? by db
        var createdAt: String? by db.auto; private set
        val createdAt2: String? by db.auto

        override fun invoke(): ValidModel {
            return ValidModel(_session, _quote)
        }

        override fun toData(): ValidData {
            return ValidData(processId, noSetter, noSetter2, title, version, unknown, createdAt, createdAt2)
        }

        companion object {
            const val tableName = "tests"
        }
    }

    class DatabaseModel(session: Session? = session(), quote: String? = null) : Model<DatabaseModel, DatabaseModel.Data>(session, "tests", false, true, quote) {
        data class Data(
            val processId: Long?,
            val modelName: String?,
            val title: String,
            val version: String,
            val ownName: String,
            val CappedName: Int,
            val ALLCAPS: Int,
            val withDigits2: Int,
            val createdAt: String?
        )

        var processId: Long? by db.key.auto
        var modelName: String? by db.key.auto
        var title: String by db
        var version: String by db
        var ownName: String by db.column("hasOwnName")
        var CappedName: Int by db
        var ALLCAPS: Int by db
        var withDigits2: Int by db
        var createdAt: String? by db.auto

        override fun invoke(): DatabaseModel {
            return DatabaseModel(_session, _quote)
        }

        override fun toData(): Data {
            return Data(processId, modelName, title, version, ownName, CappedName, ALLCAPS, withDigits2, createdAt)
        }
    }

    class TestModel(session: Session? = session(), quote: String? = null) : Model<TestModel, TestModel.Data>(session, "tests", true, false, quote) {
        data class Data(
            val processId: Long?,
            val title: String,
            val version: String,
            val batch: Int?,
            val createdAt: String?
        )

        constructor(
            processId: Long? = null,
            title: String,
            version: String,
            batch: Int? = null,
            createdAt: String? = null,
            session: Session? = session(),
            quote: String? = null
        ) : this(session, quote) {
            if (processId != null) this.processId = processId
            this.title = title
            this.version = version
            if (batch != null) this.batch = batch
            if (createdAt != null) this.createdAt = createdAt

            snapshot()
        }

        constructor(processId: Long, quote: String? = null, session: Session? = session()) : this(session, quote) {
            this.processId = processId
            snapshot()
        }

        var processId: Long? by db.autoKey; private set
        var title: String by db
        var version: String by db
        var batch: Int? by db.default
        var createdAt: String? by db.auto; private set

        override fun invoke() = TestModel(_session, _quote)

        override fun toData() = Data(processId, title, version, batch, createdAt)
    }

    class TestModelDefaultValue(session: Session? = session(), quote: String? = null) : Model<TestModelDefaultValue, TestModelDefaultValue.Data>(session, "tests", true, false, quote = quote) {
        data class Data(
            var processId: Long?,
            var title: String,
            var version: String,
            var batch: Int?,
            var createdAt: String?
        )

        constructor(
            processId: Long? = null,
            title: String,
            version: String,
            batch: Int? = null,
            createdAt: String? = null,
            session: Session? = session(),
            quote: String? = null
        ) : this(session, quote) {
            if (processId != null) this.processId = processId
            this.title = title
            this.version = version
            if (batch != null) this.batch = batch
            if (createdAt != null) this.createdAt = createdAt

            snapshot()
        }

        constructor(processId: Long) : this() {
            this.processId = processId
            snapshot()
        }

        var processId: Long? by db.autoKey; private set
        var title: String by db
        var version: String by db
        var batch: Int? by db.default(1)
        var createdAt: String? by db.auto; private set

        override fun invoke(): TestModelDefaultValue {
            return TestModelDefaultValue(_session, _quote)
        }

        override fun toData(): Data {
            return Data(processId, title, version, batch, createdAt)
        }
    }

    class TestNoNonDefaultModel(session: Session? = session(), quote: String? = null) : Model<TestNoNonDefaultModel, TestNoNonDefaultModel.Data>(session, "tests", true, false, quote = quote) {
        data class Data(
            var processId: Long?,
            var title: String?,
            var version: String?,
            var batch: Int?,
            var createdAt: String?
        )

        constructor(
            processId: Long? = null,
            title: String? = null,
            version: String? = null,
            batch: Int? = null,
            createdAt: String? = null,
            session: Session? = session(),
            quote: String? = null
        ) : this(session, quote) {
            if (processId != null) this.processId = processId
            this.title = title
            this.version = version
            if (batch != null) this.batch = batch
            if (createdAt != null) this.createdAt = createdAt

            snapshot()
        }

        constructor(processId: Long) : this() {
            this.processId = processId
            snapshot()
        }

        var processId: Long? by db.autoKey; private set
        var title: String? by db.default
        var version: String? by db.default
        var batch: Int? by db.default
        var createdAt: String? by db.auto; private set

        override fun invoke(): TestNoNonDefaultModel {
            return TestNoNonDefaultModel(_session, _quote)
        }

        override fun toData(): Data {
            return Data(processId, title, version, batch, createdAt)
        }
    }

    @Test
    fun invalidModel1() {
        thrown.expect(IllegalStateException::class.java)
        val model = InvalidModelPublicAutoKey()
    }

    @Test
    fun invalidModel2() {
        thrown.expect(IllegalStateException::class.java)
        val model = InvalidModelPublicAuto()
    }

    @Test
    fun validModel() {
        val model = ValidModel()
    }

    @Test
    fun validModelPublicAuto() {
        val model = ValidModelPublicAuto()
    }

    @Test
    fun insert_1() {
        val model = TestModel(title = "title text", version = "V1.0")
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO tests (title, version) VALUES (?, ?)", listOf("title text", "V1.0")).toString(), sql.toString());
    }

    @Test
    fun insert_1_quoted() {
        val model = TestModel(title = "title text", version = "V1.0", quote = "`")
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO `tests` (`title`, `version`) VALUES (?, ?)", listOf("title text", "V1.0")).toString(), sql.toString());
    }

    @Test
    fun insert_MissingNonDefaults() {
        val model = TestModel(5)
        model.title = "title text"
        model.batch = 4
        thrown.expect(IllegalStateException::class.java)
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO tests (title, version) VALUES (?, ?)", listOf("title text", "V1.0")).toString(), sql.toString());
    }

    @Test
    fun insert_IgnoreAutos() {
        val model = TestModel(5)
        model.title = "title text"
        model.version = "V1.0"
        model.batch = 4
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO tests (title, version, batch) VALUES (?, ?, ?)", listOf("title text", "V1.0", 4)).toString(), sql.toString());
    }

    @Test
    fun insert_Default() {
        val model = TestModel(title = "title text", version = "V1.0", batch = 4)
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO tests (title, version, batch) VALUES (?, ?, ?)", listOf("title text", "V1.0", 4)).toString(), sql.toString());
    }

    @Test
    fun insert_DefaultSkipNull() {
        val model = TestModel(title = "title text", version = "V1.0", batch = null)
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO tests (title, version) VALUES (?, ?)", listOf("title text", "V1.0")).toString(), sql.toString());
    }

    @Test
    fun insert_NoAutoColumns() {
        val model = TestModel(title = "title text", version = "V1.0", createdAt = "createdAt")
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO tests (title, version) VALUES (?, ?)", listOf("title text", "V1.0")).toString(), sql.toString());
    }

    @Test
    fun insert_DefaultSkipNullDefaultValue() {
        val model = TestModelDefaultValue(title = "title text", version = "V1.0", batch = null)
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO tests (title, version, batch) VALUES (?, ?, ?)", listOf("title text", "V1.0", 1)).toString(), sql.toString());
    }

    @Test
    fun insert_NoAutoColumnsDefaultValue() {
        val model = TestModelDefaultValue(title = "title text", version = "V1.0", createdAt = "createdAt")
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO tests (title, version, batch) VALUES (?, ?, ?)", listOf("title text", "V1.0", 1)).toString(), sql.toString());
    }

    @Test
    fun update_MissingKey() {
        thrown.expect(IllegalStateException::class.java)
        val model = TestModel(title = "title", version = "V1.0")
        val sql = model.updateQuery
    }

    @Test
    fun delete_MissingKey() {
        thrown.expect(IllegalStateException::class.java)
        val model = TestModel(title = "title", version = "V1.0")
        val sql = model.deleteQuery
    }

    @Test
    fun delete_1() {
        val model = TestModel(5)
        val sql = model.deleteQuery
        assertEquals(sqlQuery("DELETE FROM tests WHERE processId = ?", listOf(5)).toString(), sql.toString());
    }

    @Test
    fun delete_1_quoted() {
        val model = TestModel(5, quote = "`")
        val sql = model.deleteQuery
        assertEquals(sqlQuery("DELETE FROM `tests` WHERE `processId` = ?", listOf(5)).toString(), sql.toString());
    }

    @Test
    fun update_NoMods() {
        thrown.expect(IllegalStateException::class.java)
        val model = TestModel(processId = 5, title = "title", version = "V1.0")

        assertEquals(false, model.isDirty())

        val sql = model.updateQuery
    }

    @Test
    fun update_2() {
        val model = TestModel(processId = 5, title = "title", version = "V1.0")
        model.title = "title text"

        assertEquals(true, model.isDirty())
        assertEquals(true, model.isDirty(TestModel::title))
        assertEquals(false, model.isDirty(TestModel::version))

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE tests SET title = ? WHERE processId = ?", listOf("title text", 5)).toString(), sql.toString());
    }

    @Test
    fun update_2_quoted() {
        val model = TestModel(processId = 5, title = "title", version = "V1.0", quote = "`")
        model.title = "title text"

        assertEquals(true, model.isDirty())
        assertEquals(true, model.isDirty(TestModel::title))
        assertEquals(false, model.isDirty(TestModel::version))

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE `tests` SET `title` = ? WHERE `processId` = ?", listOf("title text", 5)).toString(), sql.toString());
    }

    @Test
    fun update_3() {
        val model = TestModel(processId = 5, title = "title", version = "V1.0")
        model.title = "title text"
        model.version = "V2.0"

        assertEquals(true, model.isDirty())
        assertEquals(true, model.isDirty(TestModel::title))
        assertEquals(true, model.isDirty(TestModel::version))

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE tests SET title = ?, version = ? WHERE processId = ?", listOf("title text", "V2.0", 5)).toString(), sql.toString());
    }

    @Test
    fun update_Default() {
        val model = TestModel(processId = 5, title = "title", version = "V1.0")
        model.title = "title text"
        model.version = "V2.0"
        model.batch = 4

        assertEquals(true, model.isDirty())
        assertEquals(true, model.isDirty(TestModel::title))
        assertEquals(true, model.isDirty(TestModel::version))
        assertEquals(true, model.isDirty(TestModel::batch))

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE tests SET title = ?, version = ?, batch = ? WHERE processId = ?", listOf("title text", "V2.0", 4, 5)).toString(), sql.toString());
    }

    @Test
    fun update_DefaultSkipNull() {
        val model = TestModel(processId = 5, title = "title", version = "V1.0", batch = 4)
        model.title = "title text"
        model.version = "V2.0"
        model.batch = null

        assertEquals(true, model.isDirty())
        assertEquals(true, model.isDirty(TestModel::title))
        assertEquals(true, model.isDirty(TestModel::version))
        assertEquals(true, model.isDirty(TestModel::batch))

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE tests SET title = ?, version = ? WHERE processId = ?", listOf("title text", "V2.0", 5)).toString(), sql.toString());
    }

    @Test
    fun update_DefaultSkipNullUseKey() {
        val model = TestModel(processId = 5, title = "title", version = "V1.0", batch = 4)
        model.batch = null

        assertEquals(true, model.isDirty())
        assertEquals(false, model.isDirty(TestModel::title))
        assertEquals(false, model.isDirty(TestModel::version))
        assertEquals(true, model.isDirty(TestModel::batch))

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE tests SET processId = processId WHERE processId = ?", listOf(5)).toString(), sql.toString());
    }

    @Test
    fun update_DefaultSkipNullDefaultValue() {
        val model = TestModelDefaultValue(processId = 5, title = "title", version = "V1.0", batch = 4)
        model.title = "title text"
        model.version = "V2.0"
        model.batch = null

        assertEquals(true, model.isDirty())
        assertEquals(true, model.isDirty(TestModel::title))
        assertEquals(true, model.isDirty(TestModel::version))
        assertEquals(true, model.isDirty(TestModel::batch))

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE tests SET title = ?, version = ? WHERE processId = ?", listOf("title text", "V2.0", 5)).toString(), sql.toString());
    }

    @Test
    fun update_DefaultSkipNullUseKeyDefaultValue() {
        val model = TestModelDefaultValue(processId = 5, title = "title", version = "V1.0", batch = 4)
        model.batch = null

        assertEquals(true, model.isDirty())
        assertEquals(false, model.isDirty(TestModel::title))
        assertEquals(false, model.isDirty(TestModel::version))
        assertEquals(true, model.isDirty(TestModel::batch))

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE tests SET processId = processId WHERE processId = ?", listOf(5)).toString(), sql.toString());
    }

    // no longer applicable, can always use a key column to set to itself
    //    @Test
    //    fun update_DefaultSkipNullNoUsableUnmodified() {
    //        val model = TestNoNonDefaultModel(processId = 5, title = null, version = null, batch = 4)
    //        model.batch = null
    //
    //        assertEquals(true, model.isDirty())
    //        assertEquals(false, model.isDirty(TestModel::title))
    //        assertEquals(false, model.isDirty(TestModel::version))
    //        assertEquals(true, model.isDirty(TestModel::batch))
    //
    //        thrown.expect(IllegalStateException::class.java)
    //        val sql = model.updateQuery
    //        assertEquals(sqlQuery("UPDATE tests SET title = ? WHERE processId = ?", listOf("title", 5)).toString(), sql.toString());
    //    }

    @Test
    fun test_dbCase() {
        val model = DatabaseModel()
        val columns = ArrayList<String>()
        model.forEachProp { prop, propType, columnName, value -> columns += columnName }

        assertEquals(arrayListOf("process_id", "model_name", "title", "version", "hasOwnName", "capped_name", "allcaps", "with_digits2", "created_at"), columns)
    }

    @Test
    fun test_dbCaseKeys() {
        val model = DatabaseModel()
        val columns = ArrayList<String>()
        model.forEachKey { prop, propType, columnName, value -> columns += columnName }

        assertEquals(arrayListOf("process_id", "model_name"), columns)
    }

    @Test
    fun test_dbCaseInsert() {
        val model = DatabaseModel()

        model.processId = 5L
        model.modelName = "name"
        model.title = "title"
        model.version = "version"
        model.ownName = "ownName"
        model.CappedName = 5
        model.ALLCAPS = 4
        model.withDigits2 = 3
        model.createdAt = "createdAt"

        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO tests (title, version, hasOwnName, capped_name, allcaps, with_digits2) VALUES (?, ?, ?, ?, ?, ?)", listOf("title", "version", "ownName", 5, 4, 3)).toString(), sql.toString());
    }

    @Test
    fun test_dbCaseDelete() {
        val model = DatabaseModel()

        model.processId = 5L
        model.modelName = "name"
        model.title = "title"
        model.version = "version"
        model.ownName = "ownName"
        model.CappedName = 5
        model.ALLCAPS = 4
        model.withDigits2 = 3
        model.createdAt = "createdAt"

        val sql = model.deleteQuery
        assertEquals(sqlQuery("DELETE FROM tests WHERE process_id = ? AND model_name = ?", listOf(5, "name")).toString(), sql.toString());
    }

    @Test
    fun test_dbCaseUpdate() {
        val model = DatabaseModel()

        model.processId = 5L
        model.modelName = "name"
        model.title = "title"
        model.version = "version"
        model.ownName = "ownName"
        model.CappedName = 5
        model.ALLCAPS = 4
        model.withDigits2 = 3
        model.createdAt = "createdAt"

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE tests SET title = ?, version = ?, hasOwnName = ?, capped_name = ?, allcaps = ?, with_digits2 = ? WHERE process_id = ? AND model_name = ?", listOf("title", "version", "ownName", 5, 4, 3, 5, "name")).toString(), sql.toString());
    }

    @Test
    fun test_listQuery() {
        val sql = TestModel().listQuery("single" to 10, "list" to listOf(1, 2, 3, 4))

        val sqlQuery = sqlQuery("SELECT * FROM tests WHERE single = :single AND list IN (:list)", mapOf("single" to 10, "list" to listOf(1, 2, 3, 4)))

        assertEquals(sqlQuery.toString(), sql.toString());
    }

    @Test
    fun test_listQueryQuoted() {
        val sql = TestModel(quote = "`").listQuery("single" to 10, "list" to listOf(1, 2, 3, 4))

        val sqlQuery = sqlQuery("SELECT * FROM `tests` WHERE `single` = :single AND `list` IN (:list)", mapOf("single" to 10, "list" to listOf(1, 2, 3, 4)))

        assertEquals(sqlQuery.toString(), sql.toString());
    }
}
