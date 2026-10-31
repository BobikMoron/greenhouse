import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlin.random.Random

data class SensorReading(val sensorId: String, val value: Double, val timestamp: Long)

var counter: Long = 0

fun nextTime(): Long {
    counter = counter + 1
    return counter
}


fun roundOne(value: Double): Double {
    return Math.round(value * 10.0) / 10.0
}


fun randomValue(min: Double, max: Double): Double {
    val value = min + Random.nextDouble() * (max - min)
    return roundOne(value)
}


fun minFor(sensorId: String): Double {
    if (sensorId == "HUM") return 30.0
    if (sensorId == "CO2") return 200.0
    return 15.0
}

fun maxFor(sensorId: String): Double {
    if (sensorId == "HUM") return 80.0
    if (sensorId == "CO2") return 600.0
    return 30.0
}


@Suppress("DEPRECATION_ERROR")
suspend fun emitSensorData(sensorId: String, channel: SendChannel<SensorReading>) {
    var i = 1
    while (i <= 10) {
        delay(500)

        val value = randomValue(minFor(sensorId), maxFor(sensorId))
        val reading = SensorReading(sensorId, value, nextTime())
        channel.send(reading)

        i = i + 1
    }
}


fun sensorFlow(sensorId: String): Flow<SensorReading> = flow {
    var i = 1
    while (i <= 10) {
        delay(300)

        val value = randomValue(minFor(sensorId), maxFor(sensorId))
        emit(SensorReading(sensorId, value, nextTime()))

        i = i + 1
    }
}


fun processedFlow(sensorId: String): Flow<SensorReading> {
    return sensorFlow(sensorId)
        .filter { it.value > 20.0 }
        .map { it.copy(value = roundOne(it.value * 1.1)) }
        .take(5)
        .onEach { println("Обработано: " + it) }
}


suspend fun runFlowDemo() {
    println("===== 2. FLOW =====")
    println("Создаём поток sensorFlow(\"TEMP\") ...")

    val flow = sensorFlow("TEMP")
    println("Поток создан, но НИ ОДНОГО значения ещё не пришло (Flow ленивый)")
    println()

    println("--- toList(): теперь поток начинает работать ---")
    val list = flow.toList()
    println("Собрано значений: " + list.size)
    var k = 0
    while (k < list.size) {
        println("  " + list.get(k))
        k = k + 1
    }
    println()

    println("--- Цепочка filter / map / take(5) / onEach ---")
    val processed = processedFlow("TEMP").toList()
    println("После цепочки осталось значений: " + processed.size)
    println()

    println("--- fold: сумма и среднее ---")
    val sum = processed.fold(0.0) { acc, reading -> acc + reading.value }
    var average = 0.0
    if (processed.size > 0) {
        average = sum / processed.size
    }
    println("Сумма: " + roundOne(sum))
    println("Среднее: " + roundOne(average))
    println()
}

fun main() = runBlocking {
    runChannelDemo()
    runFlowDemo()
    println("===== ГОТОВО =====")
}
