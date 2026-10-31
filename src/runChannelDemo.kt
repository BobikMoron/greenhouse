import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

@Suppress("DEPRECATION_ERROR")
suspend fun runChannelDemo() = coroutineScope {
    println("===== 1. КАНАЛ + КОРУТИНЫ =====")
    println("Запускаем 3 корутины: TEMP, HUM, CO2 (пауза 500 мс)")

    val channel = Channel<SensorReading>()

    launch { emitSensorData("TEMP", channel) }
    launch { emitSensorData("HUM", channel) }
    launch { emitSensorData("CO2", channel) }

    var count = 0
    while (count < 30) {
        val msg = channel.receive()
        println("Получено: " + msg)
        count = count + 1
    }

    channel.close()
    println("Всего получено показаний: " + count)
    println()
}
