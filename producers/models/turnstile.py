"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        super().__init__(
            "com.transitchicago.trunstile", # TODO: Come up with a better topic name
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema, # TODO: Uncomment once schema is defined
            num_partitions= 2, # TODO: num_partitions=???,
            num_replicas = 1 # TODO: num_replicas=???,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        #
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #
        #

        try:
            # emmit message to kafa "num_entries" times
            for _ in range(num_entries):
                self.producer.produce(
                        topic=self.topic_name,
                        key={"timestamp": self.time_millis()},
                        value={
                            # TODO: Configure this
                            "station_id" : self.station.station_id,
                            "station_name" : self.station.name,
                            "line" : self.station.color
                            },
                        )
            logger.info(f"{num_entries} rider entires between {timestamp} and {timestamp+time_step} reported to kafka")
            except Exception as e:
                logger.error("failed to produce message to kafka: {e}")
                logger.info("turnstile kafka integration incomplete - skipping")
