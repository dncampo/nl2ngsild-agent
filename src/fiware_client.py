import requests
from urllib.parse import urlencode
import json
import logging

logger = logging.getLogger(__name__)

class FiwareClient:
    def __init__(self, host, port):
        self.base_url = f"http://{host}:{port}"
        logger.info(f"Initialized FiwareClient with base URL: {self.base_url}")

    def execute_query(self, params):
        """
        Retrieves entities from the Context Broker.
        """
        # If params starts with 'GET', remove it and strip all whitespaces
        if isinstance(params, str) and params.strip().startswith("GET"):
            params = params.strip()[3:]  # Remove 'GET'
            params = params.strip().replace(" ", "")
        # If params starts with 'http://localhost:1026/', remove it
        if isinstance(params, str) and params.strip().startswith("http://localhost:1026/"):
            params = params.strip()[len("http://localhost:1026/"):]
            params = params.lstrip("/")            
        try:
            logger.info(f"Params: {params}")
            full_url = f"{self.base_url}/{params}"
            logger.info(f"Full URL will be: {full_url}")
            headers = {
                "Link": '<http://context/user-context.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
            }


            response = requests.get(full_url, headers=headers)
            logger.info(f"Making GET request to FIWARE Context Broker: {full_url}")
            logger.info(f"Response status code: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")

            if response.status_code != 200:
                logger.warning(f"Non-200 status code received: {response.status_code}")
                logger.warning(f"Response text: {response.text}")

            response.raise_for_status()

            response_data = response.json()
            logger.info(f"Successfully retrieved entities. Response contains {len(response_data) if isinstance(response_data, list) else 'data'}")
            logger.debug(f"Response data: {json.dumps(response_data, indent=2)}")

            return response_data

        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error when connecting to FIWARE: {e}")
            logger.error(f"Check if the FIWARE Context Broker is running at {self.base_url}")
            return e
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout error when connecting to FIWARE: {e}")
            return e
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error when connecting to FIWARE: {e}")
            logger.error(f"Response status code: {e.response.status_code if hasattr(e, 'response') else 'Unknown'}")
            logger.error(f"Response text: {e.response.text if hasattr(e, 'response') else 'Unknown'}")
            return e
        except requests.exceptions.RequestException as e:
            logger.error(f"General request error when connecting to FIWARE: {e}")
            return e
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.error(f"Response text: {response.text}")
            return e
        except Exception as e:
            logger.error(f"Unexpected error in get_entities: {e}")
            logger.exception("Full traceback:")
            return e