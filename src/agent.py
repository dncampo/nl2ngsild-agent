import configparser
import json
import logging
from dag_creator import DAGCreator
from plan_executor import PlanExecutor

logger = logging.getLogger(__name__)

def load_data_models(filepath: str) -> dict:
    """Loads Smart Data Models from a JSON file."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Error: Data models file not found at '{filepath}'")
        return {}
    except json.JSONDecodeError:
        logger.error(f"Error: Could not decode JSON from '{filepath}'")
        return {}

