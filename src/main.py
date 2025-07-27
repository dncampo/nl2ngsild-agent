import configparser
import json
import os
import logging
from dag_creator import DAGCreator
from plan_executor import PlanExecutor

def load_data_models(filepath):
    """Loads Smart Data Models from a JSON file."""
    logger = logging.getLogger(__name__)
    logger.info(f"Loading data models from: {filepath}")
    try:
        with open(os.path.join('data-models', 'user-context.jsonld'), 'r', encoding='utf-8') as f:
            data = json.load(f)
            logger.info(f"Successfully loaded data models from {filepath}")
            return data
    except FileNotFoundError:
        logger.error(f"Data models file not found at '{filepath}'")
        print(f"Error: Data models file not found at '{filepath}'")
        return None
    except json.JSONDecodeError:
        logger.error(f"Could not decode JSON from '{filepath}'")
        print(f"Error: Could not decode JSON from '{filepath}'")
        return None

def main():
    """
    Main function to run the FIWARE DAG Agent.
    """
    # Configure logging centrally for all modules
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler('ngsild_agent.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting NGSI-LD Agent")
    
    config = configparser.ConfigParser()
    config.read('config.ini')
    openai_api_key = config.get('openai', 'api_key')
    fiware_host = config.get('fiware', 'host')
    fiware_port = config.getint('fiware', 'port')

    if 'YOUR_OPENAI_API_KEY' in openai_api_key:
        print("ERROR: Please update 'config.ini' with your OpenAI API key.")
        return
    logger.info(f"Configuration loaded - FIWARE host: {fiware_host}, port: {fiware_port}")


    # 1. Load external resources
    data_models = load_data_models('data_models.json')
    if not data_models:
        logging.error("Failed to load data models. Exiting.")
        print("Failed to load data models. Exiting.")
        return

    # 2. Initialize components
    dag_creator = DAGCreator(openai_api_key)
    plan_executor = PlanExecutor(fiware_host, fiware_port)

    # 3. Get user task
    logger.info("Waiting for user input")
    user_task = input("Please enter your task: ")
    logger.info(f"Received query: {user_task}")

    # 4. Create the DAG plan structure
    dag_structure = dag_creator.create_dag_plan(user_task, data_models)
    if not dag_structure:
        logger.info("Could not create a valid DAG structure. Exiting.")
        logger.error("the dag_structure is: " + json.dumps(dag_structure, indent=2))
        return
    logger.info(f"Created DAG structure:\n {json.dumps(dag_structure, indent=2)}")

    # 5. Populate the details for each node in the DAG
    nodes = dag_creator.create_node_details(user_task, dag_structure)
    logger.info("Created nodes:\n" + json.dumps(nodes, indent=2))

    # 6. Execute the plan
    logger.info("Executing the plan")
    #plan_executor.execute_dag(dag_structure, nodes)
    logger.info("Execution completed")


if __name__ == "__main__":
    main()
