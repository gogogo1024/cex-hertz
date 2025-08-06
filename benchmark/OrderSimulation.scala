//> using dep "io.gatling:gatling-core:3.14.3"
//> using dep "io.gatling:gatling-http:3.14.3"
//> using dep "io.gatling:gatling-test-framework:3.14.3"

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.core.session.Expression  // 显式导入Expression
import scala.concurrent.duration._
import scala.util.Random

class OrderSimulation extends Simulation {

  val httpProtocol = http
    .baseUrl("http://127.0.0.1:9997")
    .wsBaseUrl("ws://127.0.0.1:9997")

  def randomUserId = "user_" + Random.alphanumeric.take(12).mkString

  val feeder = Iterator.continually(Map(
    "userId" -> randomUserId
  ))

  val wsScenario = scenario("Order WS Scenario")
    .feed(feeder)
    .exec(ws("Connect WS").connect("/ws"))
    .pause(500.milliseconds)
    // 先将随机价格和数量存入会话
    .exec(session => {
      val price = Random.between(29500, 30500).toString
      val quantity = BigDecimal(Random.between(1, 10) + Random.nextDouble())
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toString
      session.set("price", price).set("quantity", quantity)
    })
    .randomSwitch(
      50.0 -> exec(ws("Buy Order")
        .sendText("""{"action":"SubmitOrder","symbol":"BTCUSDT","side":"buy","price":"${price}","quantity":"${quantity}","user_id":"${userId}"}""")
      ),
      50.0 -> exec(ws("Sell Order")
        .sendText("""{"action":"SubmitOrder","symbol":"BTCUSDT","side":"sell","price":"${price}","quantity":"${quantity}","user_id":"${userId}"}""")
      )
    )
    .pause(1.second)
    .exec(ws("Close WS").close)

  setUp(
    wsScenario.inject(
      rampUsers(1000) during (30.seconds)
    )
  ).protocols(httpProtocol)
}
