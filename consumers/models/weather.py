"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        try:
            process_message = message.value()
            if process_message is not None:
                self.temperature = process_message["temperature"]
                self.status = process_message["status"]
                logger.info("weather process_message is complete")
            else:
                logger.info("weather process_message is empty - skipping")
        except Exception as e:
            logger.info(f"weather process_message is incomplete - {e}")
