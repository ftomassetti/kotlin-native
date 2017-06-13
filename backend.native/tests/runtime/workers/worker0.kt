import konan.worker.*

fun main(args: Array<String>) {
    val worker = startWorker()
    val future = schedule(worker, { "Input".shallowCopy().transfer()}) {
        input -> (input + " processed").transfer()
    }
    future.consume {
        result -> println("Got $result")
    }
    worker.requestTermination().consume { _ -> }
    println("OK")
}