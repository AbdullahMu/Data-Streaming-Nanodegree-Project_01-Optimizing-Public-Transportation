"""Defines a Tornado Server that consumes Kafka Event data for display"""
import logging
import logging.config
from pathlib import Path

import tornado.ioloop
import tornado.template
import tornado.web


# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")


from consumer import KafkaConsumer
from models import Lines, Weather
import topic_check


logger = logging.getLogger(__name__)
<<<<<<< HEAD

=======
WEB_SERVER_PORT = 8889
>>>>>>> parent of c05ec44 (initial commit)

class MainHandler(tornado.web.RequestHandler):
    """Defines a web request handler class"""

    template_dir = tornado.template.Loader(f"{Path(__file__).parents[0]}/templates")
    template = template_dir.load("status.html")

    def initialize(self, weather, lines):
        """Initializes the handler with required configuration"""
        self.weather = weather
        self.lines = lines

    def get(self):
        """Responds to get requests"""
        logging.debug("rendering and writing handler template")
        self.write(
            MainHandler.template.generate(weather=self.weather, lines=self.lines)
        )


def run_server():
    """Runs the Tornado Server and begins Kafka consumption"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is False:
        logger.fatal(
            "Ensure that the KSQL Command has run successfully before running the web server!"
        )
        exit(1)
<<<<<<< HEAD
    if topic_check.topic_exists("org.chicago.cta.stations.table.v1") is False:
=======

    if topic_check.topic_pattern_match("stations.table") is False:
>>>>>>> parent of c05ec44 (initial commit)
        logger.fatal(
            "Ensure that Faust Streaming is running successfully before running the web server!"
        )
        exit(1)

    weather_model = Weather()
    lines = Lines()

    application = tornado.web.Application(
        [(r"/", MainHandler, {"weather": weather_model, "lines": lines})]
    )
<<<<<<< HEAD
    application.listen(8888)
=======
    application.listen(WEB_SERVER_PORT)
>>>>>>> parent of c05ec44 (initial commit)

    # Build kafka consumers
    consumers = [
        KafkaConsumer(
<<<<<<< HEAD
            "org.chicago.cta.weather.v1",
=======
            "(\w*|\.)*weather(.(\w*|\.))*",
>>>>>>> parent of c05ec44 (initial commit)
            weather_model.process_message,
            offset_earliest=True,
        ),
        KafkaConsumer(
<<<<<<< HEAD
            "org.chicago.cta.stations.table.v1",
=======
            "(\w*|\.)*stations.table(.(\w*|\.))",
>>>>>>> parent of c05ec44 (initial commit)
            lines.process_message,
            offset_earliest=True,
            is_avro=False,
        ),
        KafkaConsumer(
<<<<<<< HEAD
            "^org.chicago.cta.station.arrivals.",
=======
            "(\w*|\.)*station.arrivals.(.(\w*|\.))*",
>>>>>>> parent of c05ec44 (initial commit)
            lines.process_message,
            offset_earliest=True,
        ),
        KafkaConsumer(
            "TURNSTILE_SUMMARY",
            lines.process_message,
            offset_earliest=True,
            is_avro=False,
        ),
    ]

    try:
        logger.info(
<<<<<<< HEAD
            "Open a web browser to http://localhost:8888 to see the Transit Status Page"
=======
            f"Open a web browser to http://localhost:{WEB_SERVER_PORT} to see the Transit Status Page"
>>>>>>> parent of c05ec44 (initial commit)
        )
        for consumer in consumers:
            tornado.ioloop.IOLoop.current().spawn_callback(consumer.consume)

        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt as e:
        logger.info("shutting down server")
        tornado.ioloop.IOLoop.current().stop()
        for consumer in consumers:
            consumer.close()


if __name__ == "__main__":
    run_server()
