import com.highmobility.mapper.mqtt.DataReducer
import com.highmobility.mapper.mqtt.DataType
import com.highmobility.mapper.mqtt.Point
import org.junit.Test

class DataReducerTest {

    @Test
    fun addsData() {
        val pointsBefore = readData("raw/fuelData.csv")

        val dataReducer = DataReducer(10, 0.5)
        val pointsAfter = arrayListOf<Point>()

        dataReducer.retained = { pointsAfter.add(it) }
        pointsBefore.forEach { dataReducer.addPoint(it) }
        dataReducer.reduce()

        assert(pointsAfter.size > 0)
        assert(pointsBefore.size > pointsAfter.size)
    }

    @Test
    fun errorAllowedChangesResult() {
        val pointsBefore = readData("raw/speedData.csv")
        val twoAllowedError = DataReducer(10, 2.0)

        var pointsAfter = arrayListOf<Point>()

        twoAllowedError.retained = {
            pointsAfter.add(it)
        }

        pointsBefore.forEach {
            twoAllowedError.addPoint(it)
        }

        val twoAllowedErrorPointsCount = pointsAfter.size

        val threeAllowedError = DataReducer(10, 3.0)
        pointsAfter = arrayListOf()

        threeAllowedError.retained = {
            pointsAfter.add(it)
        }

        pointsBefore.forEach {
            threeAllowedError.addPoint(it)
        }

        val threeAllowedErrorPointsCount = pointsAfter.size

        assert(pointsAfter.size > 0)
        assert(pointsBefore.size > pointsAfter.size)
        assert(twoAllowedErrorPointsCount > threeAllowedErrorPointsCount)
    }

    @Test
    fun printRpmReduction() {
        val pointsBefore = readData("raw/rpmData.csv")

        val dataReducer = DataReducer(10, 2.0)

        val pointsAfter = arrayListOf<Point>()

        dataReducer.retained = {
            pointsAfter.add(it)
        }

        pointsBefore.forEach {
            dataReducer.addPoint(it)
        }

        dataReducer.reduce()

        assert(pointsAfter.size > 0)
        assert(pointsBefore.size > pointsAfter.size)
    }

    @Test
    fun printSpeedReduction() {
        val pointsBefore = readData("raw/speedData.csv")

        val dataReducer = DataReducer(10, 2.0)

        val pointsAfter = mutableListOf<Point>()

        dataReducer.retained = { pointsAfter.add(it) }
        pointsBefore.forEach { dataReducer.addPoint(it) }
        dataReducer.reduce()

        assert(pointsAfter.size == 135)
        assert(pointsBefore.size > pointsAfter.size)
    }

    @Test
    fun printFuelReduction() {
        val pointsBefore = readData("raw/fuelData.csv")

        val dataReducer = DataReducer(10, 0.2)

        val pointsAfter = mutableListOf<Point>()

        dataReducer.retained = { pointsAfter.add(it) }
        pointsBefore.forEach { dataReducer.addPoint(it) }
        dataReducer.reduce()

        assert(pointsAfter.size == 204)
        assert(pointsBefore.size > pointsAfter.size)
    }

    @Test
    fun gpsDistance() {
        val p1 = Point(43.45779, -79.72129)
        val p2 = Point(43.45367, -79.72421)
        val p3 = Point(43.457947, -79.721214)

        val reducer = DataReducer(10, 250.0, DataType.GPS)
        val d = reducer.gpsDistance(p1, p2, p3)

        assert(d == 26.18499086700863)
    }

    @Test
    fun locationData() {
        val pointsBefore = readGpsData("raw/gpsData.csv")
        val dataReducer = DataReducer(10, 250.0, DataType.GPS)
        val pointsAfter = mutableListOf<Point>()

        dataReducer.retained = {
            pointsAfter.add(it)
        }

        pointsBefore.forEach {
            dataReducer.addPoint(it)
        }

        dataReducer.reduce() // final reduce for remaining points

        pointsAfter.clear()
        pointsAfter.addAll(dataReducer.retainedPoints)

        assert(pointsAfter.size == 14)
        assert(pointsBefore.size > pointsAfter.size)
    }
}