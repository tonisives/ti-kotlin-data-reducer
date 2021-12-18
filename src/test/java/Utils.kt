import com.highmobility.mapper.mqtt.LocationPoint
import com.highmobility.mapper.mqtt.Point
import java.text.ParseException
import java.text.SimpleDateFormat
import kotlin.streams.toList

fun readData(fileName: String): List<Point> {
    val reader = DataReducerTest::class.java.classLoader.getResourceAsStream(fileName)
        .bufferedReader()
    val points = arrayListOf<Point>()
    reader.lines().toList().forEachIndexed { index, it ->
        if (index > 0) {
            try {
                val split = it.split(",")
                points.add(Point(split[0].toDouble(), split[1].toDouble()))
            } catch (e: NumberFormatException) {

            }
        }
    }


    return points
}

fun readGpsData(fileName: String): List<LocationPoint> {
    val format = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSX")

    val reader = DataReducerTest::class.java.classLoader.getResourceAsStream(fileName)
        .bufferedReader()
    val points = arrayListOf<LocationPoint>()
    reader.lines().toList().forEachIndexed { index, it ->
        if (index > 0) {
            try {
                val split = it.split(",")
                val timeString = split[0]
                val time = format.parse(timeString).time.toDouble()
                points.add(LocationPoint(split[1].toDouble(), split[2].toDouble(), time))
            } catch (e: NumberFormatException) {
                println("NumberFormatException: $e")
            } catch (e: ParseException) {
                println("ParseException: $e")
            }
        }
    }

    return points
}
