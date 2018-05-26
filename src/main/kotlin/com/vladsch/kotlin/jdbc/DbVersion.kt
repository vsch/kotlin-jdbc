package com.vladsch.kotlin.jdbc

class DbVersion private constructor(val major: Int, val minor: Int?, val patch: Int?, val metadata: String?, dummy: Any?) : Comparable<DbVersion> {
    constructor(major: Int) : this(major, null, null, null, null)
    constructor(major: Int, minor: Int) : this(major, minor, null, null, null)
    constructor(major: Int, minor: Int, patch: Int) : this(major, minor, patch, null, null)
    constructor(major: Int, minor: Int, patch: Int, metadata: String) : this(major, minor, patch, metadata, null)

    override fun compareTo(other: DbVersion): Int {
        return if (major == other.major) {
            if (minor == other.minor) {
                if (minor == null || other.minor == null) 0
                else {
                    if (patch == other.patch) {
                        if (patch == null || other.patch == null) 0
                        else {
                            if (metadata == other.metadata) 0
                            else {
                                if (metadata == null) -1
                                else if (other.metadata == null) 1
                                else metadata.compareTo(other.metadata)
                            }
                        }
                    } else {
                        if (patch == null) -1
                        else if (other.patch == null) 1
                        else patch.compareTo(other.patch)
                    }
                }
            } else {
                if (minor == null) -1
                else if (other.minor == null) 1
                else minor.compareTo(other.minor)
            }
        } else {
            major.compareTo(other.major)
        }
    }

    /**
     * compare with nulls matching all versions
     *
     * @param other DbVersion
     * @return Boolean  true if this version selects other
     */
    fun selects(other: DbVersion): Boolean {
        return 0 == if (major == other.major) {
            if (minor == other.minor) {
                if (minor == null || other.minor == null) 0
                else {
                    if (patch == other.patch) {
                        if (patch == null || other.patch == null) 0
                        else {
                            if (metadata == other.metadata) 0
                            else {
                                if (metadata == null) 0
                                else if (other.metadata == null) 1
                                else metadata.compareTo(other.metadata)
                            }
                        }
                    } else {
                        if (patch == null) 0
                        else if (other.patch == null) 1
                        else patch.compareTo(other.patch)
                    }
                }
            } else {
                if (minor == null) 0
                else if (other.minor == null) 1
                else minor.compareTo(other.minor)
            }
        } else {
            major.compareTo(other.major)
        }
    }

    fun nextMajor():DbVersion {
        return DbVersion(major+1, if (minor == null) null else 0,if (patch == null) null else 0, null,null)
    }

    fun nextMinor():DbVersion {
        return DbVersion(major, (minor ?: 0) + 1,if (patch == null) null else 0, null,null)
    }

    fun nextPatch(metadata: String? = null):DbVersion {
        return DbVersion(major, minor ?: 0, (patch ?: 0) + 1, metadata, null)
    }

    fun withMetadata(metadata: String):DbVersion {
        return DbVersion(major, minor ?: 0, patch ?: 0, metadata)
    }

    override fun toString(): String {
        val sb = StringBuilder()
        sb.append('V').append(major)
        if (minor != null) {
            sb.append('_').append(minor)
            if (patch != null) {
                sb.append('_').append(patch)
                if (metadata != null) {
                    sb.append('_').append(metadata)
                }
            }
        }
        return sb.toString()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DbVersion) return false

        if (major != other.major) return false
        if (minor != other.minor) return false
        if (patch != other.patch) return false
        if (metadata != other.metadata) return false

        return true
    }

    override fun hashCode(): Int {
        var result = major
        result = 31 * result + (minor ?: 0)
        result = 31 * result + (patch ?: 0)
        result = 31 * result + (metadata?.hashCode() ?: 0)
        return result
    }

    companion object {
        const val pattern = "^(V\\d+(?:_\\d+(?:_\\d+(?:_.*)?)?)?)$"
        val regex = pattern.toRegex()

        fun of(version: String): DbVersion {
            if (!version.matches(regex)) {
                throw IllegalArgumentException("Invalid version format: $version, expected regex match /$pattern/")
            }

            val parts = version.removePrefix("V").split('_', limit = 4)

            return when (parts.size) {
                1 -> DbVersion(parts[0].toInt())
                2 -> DbVersion(parts[0].toInt(), parts[1].toInt())
                3 -> DbVersion(parts[0].toInt(), parts[1].toInt(), parts[2].toInt())
                4 -> DbVersion(parts[0].toInt(), parts[1].toInt(), parts[2].toInt(), parts[3])
                else -> DbVersion(0, 0, 0)
            }
        }
    }
}
