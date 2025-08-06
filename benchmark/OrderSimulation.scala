import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._
import scala.util.Random

class OrderSimulation extends Simulation {

  val httpProtocol = http
    .baseUrl("http://127.0.0.1:9997") // 仅用于握手
    .wsBaseUrl("ws://127.0.0.1:9997")

  def randomUserId = "user_" + Random.alphanumeric.take(12).mkString

  val feeder = Iterator.continually(Map(
    "userId" -> randomUserId
  ))

  val wsScenario = scenario("Order WS Scenario")
    .feed(feeder)
    .exec(ws("Connect WS").connect("/ws"))
    .pause(0.5)
    .randomSwitch(50.0 -> exec(ws("Buy Order")
      .sendText(
        s"""{"action":"SubmitOrder","symbol":"BTCUSDT","side":"buy","price":"${scala.util.Random.between(29500, 30500)}","quantity":"${BigDecimal(scala.util.Random.between(1, 10) + scala.util.Random.nextDouble()).setScale(2, BigDecimal.RoundingMode.HALF_UP)}","user_id":"${userId}"}"""
      )
    ),
    50.0 -> exec(ws("Sell Order")
      .sendText(
        s"""{"action":"SubmitOrder","symbol":"BTCUSDT","side":"sell","price":"${scala.util.Random.between(29500, 30500)}","quantity":"${BigDecimal(scala.util.Random.between(1, 10) + scala.util.Random.nextDouble()).setScale(2, BigDecimal.RoundingMode.HALF_UP)}","user_id":"${userId}"}"""
      )
    ))
    .pause(1)
    .exec(ws("Close WS").close)

  setUp(
    wsScenario.inject(
      rampUsers(1000) during (30.seconds) // 30秒内逐步启动1000用户
    )
  ).protocols(httpProtocol)
}
