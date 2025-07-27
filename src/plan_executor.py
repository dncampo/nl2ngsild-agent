import time
import logging
from typing import Dict, List
from dag_node import DAGNode
# We would have a fiware_client here as in the previous version
# from fiware_client import FiwareClient


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

    def execute_dag(self, dag_nodes_structure: Dict[str, List[str]], nodes: Dict[str, DAGNode]):
        """
        Executes the DAG in topological order.

        Args:
            dag_structure: The adjacency list of the DAG.
            nodes: A dictionary of DAGNode objects with populated details.
        """
        dag_structure = dag_nodes_structure['dag']
        logger.info("\n--- Starting DAG Execution ---")
        # Simple topological sort (assumes no cycles)
        in_degree = {u: 0 for u in dag_structure}
        for u in dag_structure:
            for v in dag_structure[u]:
                in_degree[v] += 1

        queue = [u for u in dag_structure if in_degree[u] == 0]
        execution_order = []
        while queue:
            u = queue.pop(0)
            execution_order.append(u)
            for v in dag_structure[u]:
                in_degree[v] -= 1
                if in_degree[v] == 0:
                    queue.append(v)

        logger.info(f"Execution Order: {execution_order}")

        nodes_details = dag_nodes_structure['nodes']
        for node_id in execution_order:
            node.id = nodes[node_id]
            node.description = nodes_details[node_id]['description']
            node.atomic = nodes_details[node_id]['atomic']
            node.query = nodes_details[node_id]['query']
            node.status = "running"
            logger.info(f"\nExecuting Node: {node.id} ('{node.goal_description}')")

            # Here you would add the logic to interact with the FIWARE Context Broker
            # based on the node's goal. This is where you'd call the LLM to generate
            # the specific NGSI-LD query for this node's sub-task.

            # --- Placeholder for FIWARE interaction ---
            try:
                logger.info(f"  > Simulating interaction with FIWARE Context Broker...")
                time.sleep(1) # Simulate network latency
                # In a real scenario, you'd generate and execute a query here.
                # The result would be stored in node.output_data
                node.output_data = {"result": f"Data for node {node.id}"}
                node.status = "completed"
                logger.info(f"  > Node {node.id} completed successfully.")
            except Exception as e:
                node.status = "failed"
                node.error = str(e)
                logger.error(f"  > Node {node.id} failed: {node.error}")
                # Here you could trigger a re-planning step for this specific node.
                # The re-planning would take the error into account.
                break # Stop execution on failure for this simple scaffold

        logger.info("\n--- DAG Execution Finished ---")
        final_output_node_id = execution_order[-1]
        final_result = nodes[final_output_node_id].output_data
        logger.info(f"Final Result from node '{final_output_node_id}': {final_result}")
        return final_result
