import time
import logging
from typing import Dict, List
from dag_node import DAGNode
from fiware_client import FiwareClient
from dag_creator import DAGCreator
import openai

logger = logging.getLogger(__name__)

class PlanExecutor:
    """
    Executes the DAG plan, iterating over the graph and interfacing with the
    FIWARE Context Broker for each step. It can handle errors and potentially
    re-plan a sub-task if it fails.
    """
    def __init__(self, fiware_host: str, fiware_port: int):
        # self.fiware_client = FiwareClient(fiware_host, fiware_port)
        logger.info(f"PlanExecutor initialized to connect to FIWARE at {fiware_host}:{fiware_port}")
        pass

    def execute_dag(self, dag_nodes_structure: Dict[str, List[str]], nodes: Dict[str, DAGNode], fiware_host: str, fiware_port: int, user_task: str, dag_creator: DAGCreator):
        """
        Executes the DAG in topological order.

        Args:
            dag_structure: The adjacency list of the DAG.
            nodes: A dictionary of DAGNode objects with populated details.
        """
        logger.info("\n--- Starting DAG Execution ---")
        # Simple topological sort (assumes no cycles)
        in_degree = {u: 0 for u in dag_nodes_structure}
        for u in dag_nodes_structure:
            in_degree[u] = len(dag_nodes_structure[u]['depends'])
        logger.info(f"In degree: {in_degree}")

        # Create an adjacency matrix from the adjacency list (dag_nodes_structure)
        node_ids = list(dag_nodes_structure.keys())
        node_index = {node_id: idx for idx, node_id in enumerate(node_ids)}
        n = len(node_ids)
        adjacency_matrix = [[0 for _ in range(n)] for _ in range(n)]
        for u in dag_nodes_structure:
            for v in dag_nodes_structure[u]['depends']:
                i = node_index[u]
                j = node_index[v]
                adjacency_matrix[j][i] = 1
        logger.info("Adjacency Matrix:")
        for row in adjacency_matrix:
            logger.info(row)

        # Perform topological sort using the adjacency matrix
        # We'll use Kahn's algorithm, but with the adjacency matrix for reference

        # Compute in-degree from adjacency matrix
        in_degree_matrix = [0] * n
        for j in range(n):
            for i in range(n):
                if adjacency_matrix[i][j]:
                    in_degree_matrix[j] += 1

        # Map index back to node_id
        index_to_node_id = {idx: node_id for node_id, idx in node_index.items()}

        # Initialize queue with nodes of in-degree 0
        topo_queue = [idx for idx, deg in enumerate(in_degree_matrix) if deg == 0]
        topo_order = []
        in_degree_matrix_copy = in_degree_matrix[:]
        adjacency_matrix_copy = [row[:] for row in adjacency_matrix]

        while topo_queue:
            u_idx = topo_queue.pop(0)
            topo_order.append(index_to_node_id[u_idx])
            for v_idx in range(n):
                if adjacency_matrix_copy[u_idx][v_idx]:
                    in_degree_matrix_copy[v_idx] -= 1
                    if in_degree_matrix_copy[v_idx] == 0:
                        topo_queue.append(v_idx)

        logger.info(f"Topological order from adjacency matrix: {topo_order}")


        for node_id in topo_order:
            fiware_client = FiwareClient( fiware_host, fiware_port )

            logger.info(f"Executing Node: {node_id}")
            node_details = dag_nodes_structure[node_id]
            logger.info(f"Node details: {node_details}")
            if node_details['atomic']:
                logger.info(f"  > FIWARE client initialized")
                response = fiware_client.execute_query(node_details['query'])
                dag_nodes_structure[node_id]['output'] = response
                logger.info(f"  > Response: {response}")
                logger.info(f"  > Node {node_id} is atomic. Executing query: {node_details['query']}")
                previous_output = response

            else:
                logger.info(f"  > Node {node_id} is not atomic. Call LLM to solve this sub step.")
                # Compose prompt for LLM
                prompt = node_details.get('description', f"Process step: {node_id}")
                # Use previous_output as context
                # Replace the following line with your actual LLM call
                # For example: llm_response = self.llm(prompt, previous_output)
                # Here, we'll just mock the LLM call for demonstration
                new_prompt = f"current task step is: {prompt} with context from information gathered from the previous step: {previous_output}. The original task is: {user_task} and the previous output is the context of the previous node and you should use it to solve this actual step. The whole plan is: {dag_nodes_structure}. Give me an atomic step to directly query the context broker."
                logger.info(f"  > Prompt to LLM: {new_prompt}")
                try:
                    llm_response = dag_creator.send_message_to_llm(new_prompt)
                    response_CB = fiware_client.execute_query(llm_response)
                    dag_nodes_structure[node_id]['output'] = response_CB
                    logger.info(f"  > Response from CB: {response_CB}")
                    previous_output = response_CB
                except Exception as e:
                    logger.error(f"Error creating DAG plan: {e}. Returning empty dictionary.")
                    return {}

                dag_nodes_structure[node_id]['output'] = llm_response
                logger.info(f"  > LLM response: {llm_response}")
                previous_output = llm_response

            message_to_llm = "Given the step: " + str(node_details) + \
                    " .The output from the context broker is: " + str(previous_output) + \
                    ". If this is an error, probably we need to redo this step. If the resultset is empty, it could be a malformed query that wrongly queried the attribute. Anyway, if there is some data you might use it to solve the next step. Check if the ID or other attribute is used in the next step."
            logger.info(f"  > Message to LLM: {message_to_llm}")
            llm_response = dag_creator.send_message_to_llm(message_to_llm)
            dag_nodes_structure[node_id]['output'] = llm_response
            logger.info(f"  > LLM response: {llm_response}")
            previous_output = llm_response

        logger.info(f"DAG nodes structure: {dag_nodes_structure}")
        logger.info(f"result of the last node: {dag_nodes_structure[topo_order[-1]]['output']}")
        logger.info("\n--- DAG Execution Finished ---")

        return dag_nodes_structure[topo_order[-1]]['output']
