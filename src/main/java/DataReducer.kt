package com.highmobility.mapper.mqtt

import kotlin.math.abs
import kotlin.math.cos
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * Call [addPoint] to add points. Listen for [retained] to get reduced points.
 *
 * When finished recording data(for instance engine off), don't forget to call the final [reduce],
 * which will send the last remaining data to the [retained] callback.
 *
 * Use a bigger [allowedError] to retain more points.
 */
class DataReducer(
    bufferSize: Int = 10,
    private var allowedError: Double = 2.0,
    private val dataType: DataType = DataType.SINGLE_VALUE
) {
    var retainedPoints = mutableListOf<Point>()

    class Buffer(val size: Int) {
        val points = arrayOfNulls<Point>(size)
        var count = 0

        fun add(point: Point) {
            if (points.last() != null) {
                println("DataReducer: buffer full")
            } else {
                points[count++] = point
            }
        }

        fun clear() {
            for (i in points.indices) {
                points[i] = null
            }
            count = 0
        }

        fun isFull() = points.last() != null

        override fun toString(): String {
            return "Buffer [${points.joinToString()}]"
        }
    }

    private val buffer = Buffer(bufferSize)
    var retained: ((Point) -> Unit)? = null

    /** Add a point to the buffer. Use either [Point] or [LocationPoint]. */
    fun addPoint(point: Point) {
        if (buffer.count == 0) {
            buffer.add(point)
            addRetainedPoint(point)
        } else {
            buffer.add(point)
        }

        if (buffer.isFull()) {
            reduce()
        }
    }

    private fun addRetainedPoint(point: Point) {
        retainedPoints.add(point)
        this.retained?.let { it(point) }
    }

    private fun saveRetainedPoints(): Int {
        var indexOfLastAddedPoint = 0
        for (i in (1 until buffer.count)) {
            if (buffer.points[i]?.retain == true) {
                addRetainedPoint(buffer.points[i]!!)
                indexOfLastAddedPoint = i
            }
        }

        buffer.clear()

        return indexOfLastAddedPoint
    }

    /**
     * Reduces the buffer with RDP and clears it. Next points will be added to an empty buffer. Will
     * be called automatically when buffer is full.
     *
     * @return The reduced points.
     */
    fun reduce() {
        if (buffer.count <= 2) {
            println("minimum 3 points required for reduction")
            return
        }

        val bufferCopy = buffer.points.filterNotNull()

        douglasPeucker(0, buffer.count - 1)

        val lastSavedPointIndex = saveRetainedPoints()

        val higherErrorPointIndex =
            getPointWithHighestError(lastSavedPointIndex, bufferCopy.count() - 1, bufferCopy)

        bufferCopy[lastSavedPointIndex]?.retain = false
        bufferCopy[higherErrorPointIndex]?.retain = false

        var dropPoint = false
        var pointToDropIndex = 0

        if (buffer.size - lastSavedPointIndex == buffer.size) {
            val lastPoint = bufferCopy.last()!!
            val lastSavedPoint = bufferCopy[lastSavedPointIndex]!!
            val pointSpread =
                ((lastPoint.timestamp - lastSavedPoint.timestamp) % (buffer.size - 3)).toInt() +
                        1
            pointToDropIndex = lastSavedPointIndex + pointSpread

            if (pointToDropIndex == higherErrorPointIndex || pointToDropIndex == lastSavedPointIndex
            ) {
                pointToDropIndex++
            }

            dropPoint = true
        }

        for (i in lastSavedPointIndex until bufferCopy.size) {
            if (dropPoint && i == pointToDropIndex) continue
            buffer.add(bufferCopy[i]!!)
        }
    }

    // https://github.com/Geotab/curve/blob/main/bufferedCurve.py#L326
    private fun douglasPeucker(firstIndex: Int, lastIndex: Int) {
        if (lastIndex - firstIndex <= 1) return

        // Find the point with the maximum distance
        var dmax = 0.0
        var indexOfLargestDistance = firstIndex

        // Start
        val startPoint = buffer.points[firstIndex]!!
        val endPoint = buffer.points[lastIndex]!!

        for (i in indexOfLargestDistance..lastIndex) {
            val currentPoint = buffer.points[i]!!
            val d = distance(currentPoint, startPoint, endPoint)

            if (d > dmax) {
                indexOfLargestDistance = i
                dmax = d
            }
        }

        // If max distance is greater than allowed error, recursively simplify
        if (dmax > allowedError) {
            // Recursive call if one point has too big distance
            buffer.points[indexOfLargestDistance]?.retain = true
            douglasPeucker(indexOfLargestDistance, lastIndex)
            douglasPeucker(firstIndex, indexOfLargestDistance)
        } else {
            for (i in (firstIndex + 1) until lastIndex) {
                buffer.points[i]?.retain = false
            }
        }
    }

    /** Distance calculation methods */
    private fun getPointWithHighestError(
        firstPoint: Int,
        lastPoint: Int,
        buffer: List<Point?>
    ): Int {
        var maxDistance = 0.0
        var maxDistanceIndex = buffer.size - 1

        for (i in (firstPoint + 1) until lastPoint) {
            val d = distance(buffer[i]!!, buffer[firstPoint]!!, buffer[lastPoint]!!)

            if (d > maxDistance) {
                maxDistance = d
                maxDistanceIndex = i
            }
        }
        return maxDistanceIndex
    }

    private fun distance(current: Point, start: Point, end: Point): Double {
        return if (dataType == DataType.GPS) {
            gpsDistance(current, start, end)
        } else {
            perpendicularDistance(
                current.value,
                current.timestamp,
                start.value,
                start.timestamp,
                end.value,
                end.timestamp
            )
        }
    }

    private fun distanceBetweenPoints(vx: Double, vy: Double, wx: Double, wy: Double): Double {
        return (vx - wx).pow(2) + (vy - wy).pow(2)
    }

    private fun distanceToSegmentSquared(
        px: Double,
        py: Double,
        vx: Double,
        vy: Double,
        wx: Double,
        wy: Double
    ): Double {
        val l2 = distanceBetweenPoints(vx, vy, wx, wy)
        if (l2 == 0.0) return distanceBetweenPoints(px, py, vx, vy)
        val t = ((px - vx) * (wx - vx) + (py - vy) * (wy - vy)) / l2
        if (t < 0) return distanceBetweenPoints(px, py, vx, vy)
        return if (t > 1) distanceBetweenPoints(px, py, wx, wy)
        else distanceBetweenPoints(px, py, vx + t * (wx - vx), vy + t * (wy - vy))
    }

    private fun perpendicularDistance(
        px: Double,
        py: Double,
        vx: Double,
        vy: Double,
        wx: Double,
        wy: Double
    ): Double {
        return sqrt(distanceToSegmentSquared(px, py, vx, vy, wx, wy))
    }

    private fun estimateLatLongDistance(
        startLat: Double,
        startLong: Double,
        endLat: Double,
        endLong: Double
    ): Double {
        // Estimate the distance between points
        // https://www.movable-type.co.uk/scripts/latlong.html
        val startLatRad = startLat * (Math.PI / 180)
        val endLatRad = endLat * (Math.PI / 180)
        val startLongRad = startLong * (Math.PI / 180)
        val endLongRad = endLong * (Math.PI / 180)

        val x = (endLongRad - startLongRad) * cos((startLatRad + endLatRad) / 2)
        val y = endLatRad - startLatRad
        return sqrt(x.pow(2) + y.pow(2))
    }

    fun gpsDistance(current: Point, start: Point, end: Point): Double {
        val area =
            abs(
                (start.timestamp * (end.value - current.value) +
                        current.timestamp * (start.value - end.value) +
                        end.timestamp * (current.value - start.value)) *
                        cos(((start.value + end.value) / 2)) *
                        (Math.PI / 180)
            )

        if (area == 0.0) return 0.0

        val base = estimateLatLongDistance(start.value, start.timestamp, end.value, end.timestamp)

        var distance =
            if (base > 0) {
                area / base
            } else {
                0.0
            }

        distance *= 1e6

        return distance
    }
}

// value == lat, timestamp = long
open class Point(
    val value: Double,
    val timestamp: Double,
) {
    var retain = false

    override fun toString(): String {
        return "${value}:${timestamp}${if (retain) " retain" else ""}"
    }
}

// don't override [Point.timestamp]. it is used for calculations
class LocationPoint(
    val lat: Double,
    val long: Double,
    val time: Double,
) : Point(lat, long)

enum class DataType {
    SINGLE_VALUE,
    GPS
}
