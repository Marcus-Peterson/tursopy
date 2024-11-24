#Manages logging.

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TursoLogger:
    @staticmethod
    def log_query(sql, args=None):
        logging.info(f"Executing SQL: {sql}")
        if args:
            logging.info(f"With args: {args}")

    @staticmethod
    def log_response(response):
        logging.info(f"Response: {response}")
