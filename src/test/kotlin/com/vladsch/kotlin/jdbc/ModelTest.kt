package com.vladsch.kotlin.jdbc

import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import kotlin.test.assertEquals

@Suppress("UNUSED_VARIABLE")
class ModelTest {
    @Rule
    @JvmField
    var thrown = ExpectedException.none()

    class InvalidModelPublicAutoKey() : Model<InvalidModelPublicAutoKey>("tests", false) {
        var processId: Long? by model.auto.key
        var title: String by model
        var version: String by model
        var unknown: String? by model
        var createdAt: String? by model.auto; private set

        companion object {
            val fromRow: (Row) -> InvalidModelPublicAutoKey = { row ->
                InvalidModelPublicAutoKey().load(row)
            }
        }
    }

    class InvalidModelPublicAuto() : Model<InvalidModelPublicAuto>("tests", false) {
        var processId: Long? by model.auto.key; private set
        var title: String by model
        var version: String by model
        var unknown: String? by model
        var createdAt: String? by model.auto

        companion object {
            val fromRow: (Row) -> InvalidModelPublicAutoKey = { row ->
                InvalidModelPublicAutoKey().load(row)
            }
        }
    }

    class ValidModelPublicAuto : Model<ValidModelPublicAuto>("tests") {
        var processId: Long? by model.auto.key
        var title: String by model
        var version: String by model
        var unknown: String? by model
        var createdAt: String? by model.auto

        companion object {
            val fromRow: (Row) -> InvalidModelPublicAutoKey = { row ->
                InvalidModelPublicAutoKey().load(row)
            }
        }
    }

    class ValidModel() : Model<ValidModel>("tests", false) {
        var processId: Long? by model.key.auto; private set
        val noSetter: String by model.auto
        val noSetter2: String by model.auto.key
        var title: String by model
        var version: String by model
        var unknown: String? by model
        var createdAt: String? by model.auto; private set
        val createdAt2: String? by model.auto

        companion object {
            val fromRow: (Row) -> ValidModel = { row ->
                ValidModel().load(row)
            }
        }
    }

    class TestModel() : Model<TestModel>("tests", false) {
        constructor(
            processId: Long? = null,
            title: String,
            version: String,
            batch: Int? = null,
            createdAt: String? = null
        ) : this() {
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

        var processId: Long? by model.auto.key; private set
        var title: String by model
        var version: String by model
        var batch: Int? by model.default
        var createdAt: String? by model.auto; private set

        companion object {
            val fromRow: (Row) -> TestModel = { row ->
                TestModel().load(row)
            }
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
        assertEquals(sqlQuery("INSERT INTO `tests` (`title`, `version`) VALUES (?, ?)", listOf("title text", "V1.0")).toString(), sql.toString());
    }

    @Test
    fun insert_MissingNonDefaults() {
        val model = TestModel(5)
        model.title = "title text"
        model.batch = 4
        thrown.expect(IllegalStateException::class.java)
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO `tests` (`title`, `version`) VALUES (?, ?)", listOf("title text", "V1.0")).toString(), sql.toString());
    }

    @Test
    fun insert_IgnoreAutos() {
        val model = TestModel(5)
        model.title = "title text"
        model.version = "V1.0"
        model.batch = 4
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO `tests` (`title`, `version`, `batch`) VALUES (?, ?, ?)", listOf("title text", "V1.0", 4)).toString(), sql.toString());
    }

    @Test
    fun insert_Default() {
        val model = TestModel(title = "title text", version = "V1.0", batch = 4)
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO `tests` (`title`, `version`, `batch`) VALUES (?, ?, ?)", listOf("title text", "V1.0", 4)).toString(), sql.toString());
    }

    @Test
    fun insert_NoAutoColumns() {
        val model = TestModel(title = "title text", version = "V1.0", createdAt = "createdAt")
        val sql = model.insertQuery
        assertEquals(sqlQuery("INSERT INTO `tests` (`title`, `version`) VALUES (?, ?)", listOf("title text", "V1.0")).toString(), sql.toString());
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
        assertEquals(sqlQuery("UPDATE `tests` SET `title` = ?, `version` = ? WHERE `processId` = ?", listOf("title text", "V2.0", 5)).toString(), sql.toString());
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
        assertEquals(true, model.isDirty(TestModel::version))

        val sql = model.updateQuery
        assertEquals(sqlQuery("UPDATE `tests` SET `title` = ?, `version` = ?, `batch` = ? WHERE `processId` = ?", listOf("title text", "V2.0", 4, 5)).toString(), sql.toString());
    }
}
