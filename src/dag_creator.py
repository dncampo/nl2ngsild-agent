import json
import logging
from typing import Dict, List
import openai
from dag_node import DAGNode

logger = logging.getLogger(__name__)

class DAGCreator:
    """
    Creates a Directed Acyclic Graph (DAG) plan based on the user's task.
    This component is responsible for breaking down a complex task into smaller,
    manageable sub-tasks represented as nodes in a graph.
    """
    def __init__(self, openai_api_key: str):
        openai.api_key = openai_api_key

    def create_dag_plan(self, user_task: str, context: Dict) -> Dict[str, List[str]]:
        """
        Uses an LLM to generate a DAG structure in plain text/JSON.
        The DAG represents the dependencies between sub-tasks.

        Args:
            user_task: The high-level task from the user.
            data_models: The available FIWARE Smart Data Models.

        Returns:
            A dictionary representing the adjacency list of the DAG.
            e.g., {"a": ["b", "c"], "b": ["d"], "c": ["d"], "d": []}
        """
        logger.info("Creating DAG plan...")
        prompt = f"""
        You are an AI planner that helps to create plans to query a FIWARE Context Broker.
        Based on this, create a plan to retrieve the requested information from the user.
        Your job is to create a dependency graph of sub-tasks with the minimum amount of steps to accomplish a user's goal.
        The user's task is: "{user_task}"

        The available FIWARE data models are:
        {json.dumps(context['data_models'], indent=2)}

        Based on the user's task, break it down into a series of smaller, dependent steps. Each step should be atomic, 
        meaning that it can be executed as a single query to the Context Broker. If it is not atomic, you should return a prompt to the LLM 
        to solve this sub step.
        Represent the plan as a JSON object where keys are node IDs (e.g., "a", "b", "c") and
        values are lists of node IDs that depend on the key.
        This represents a Directed Acyclic Graph (DAG).

        For example, for the task "Find the average temperature of all sensors in Building 1", a plan might be:
        1. "a": Find all 'Building' entities with name 'Building 1'.
        2. "b": From the result of "a", get the list of all associated 'Device' entities (sensors).
        3. "c": For each sensor from "b", get its 'Temperature' reading.
        4. "d": Calculate the average of all temperatures from "c".

        The corresponding JSON DAG would be:
        dag: {{
          "a": {{
                "desciption": "Find all 'Building' entities with name 'Building 1'",
                "depends": [],
                "atomic": true,
                "query": "GET http://{context['fiware_host']}:{context['fiware_port']}/ngsi-ld/v1/entities/?type=Building&q=name==%22Building%201%22"
             }},
          "b": {{
                "desciption": "From the result of 'a', get the list of all associated 'Device' entities (sensors)",
                "depends": ["a"],
                "atomic": true,
                "query": "GET http://{context['fiware_host']}:{context['fiware_port']}/ngsi-ld/v1/entities/?type=Device&q=building==%22Building%201%22"
          }},
          "c": {{
                "desciption": "For each sensor from 'b', get its 'Temperature' reading",
                "depends": ["b"],
                "atomic": true,
                "query": "GET http://{context['fiware_host']}:{context['fiware_port']}/ngsi-ld/v1/entities/?type=Device&q=name==%22Sensor%201%22&attrs=temperature"
          }},
          "d": {{
                "desciption": "Calculate the average of all temperatures from 'c'",
                "depends": ["c"],
                "atomic": true,
                "query": "GET http://{context['fiware_host']}:{context['fiware_port']}/ngsi-ld/v1/entities/?type=Device&q=name==%22Sensor%201%22&attrs=temperature"
          }}
        }}

        In the field "depends" you should only include the node IDs that are necessary to execute the step.


        Try to create a DAG with the minimum amount of steps possible and the maximum amount of atomic steps possible. Just explicitly explain the reasoning process to create the DAG and devise a plan that once the DAG is linearly executed, it will lead to the user's goal.  

        Return ONLY the JSON object representing the DAG in dag.
        The outout SHOULD BE in JSON format. Do not include any other text or comments nor characters. NO markdown please.

        If you find that one of the intermediary steps of the DAG plan are atomic and directly solvable performing a query to the FIWARE Context Broker, you can use the following rationale:
        you always think about what are the entity types and attributes names that need to be queried, in order to perform a non malformed query. For doing so, you have to see the context taking the
        context file and checking for data types and attribute names. For certain entity types, you will find the defined term followed by a link. You follow this link to find the definition
        of this entity type on a repository and further check their attributes. Here is an example: if someone wants to know about the health of the cow named Bumble, you might find that the context file
        has a definition of type Animal, which is defined in "agrifood": "https://smartdatamodels.org/dataModel.Agrifood/". Moreover, if you follow this link about Agrifood you will find a repository having 
        the searched term Animal. If you enter this link, you land in a page defining the attributes of Animal. Either you have enough with this, or you enter the definition of each attribute and see how is defines. 
        You can find the details of this in the file called schema.json. In there you will see that this healthCondition can hold a string enumerating one of the following one of the possible statuses: healthy, inTreatment, sick. 
        However, for the original question it is no necessary to go that deep in the graph of context, since the only needed thing to answer the query is that the cow called Bumble is represented by an entity of type Animal 
        and the searched attribute is called healthCondition. Also, you can see that the Animal entity type has an attribute called name. In this particular case, you can see that in the Context Broker there is an entity of type Animal 
        looking at the result of the GET http://<CB_ADDRESS>:<CB_PORT>/ngsi-ld/v1/types. With this query you obtain the list definition of types stored in the context broker. Finally, looking at this information and to the context 
        returned by the agrifood:Animal data type, you can infer that the cow is of type Animal and has attributes called name and healthCondition, inferring that the query to the context broker can be performed in one step and should be 
        something like GET http://<CB_ADDRESS>:<CB_PORT>/ngsi-ld/v1/entities/?type=Animal&q=name==%22Bumble%22&attrs=healthCondition. 
        In other cases it might be needed to use a particular context file for this query, adding something like 'Link: <http://context/user-context.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json' in the header of the request.
        Adding this last one, the query will be executed using the user-context.jsonld file. Most of the cases it is better to get beforehand information about this, or asking the user, because the file used could be with any name.

        This is just an example, so you need to ALWAYS navigate the graph of context and SDM data types to find the entity types and attributes names.
        Somethimes the attribute that relates an entity to another entity is not a simple attribute, but a relationship. In this case, you need to find the entity type of the related entity and the attribute that relates the two entities. In other cases
        you have a third entity that relates other two entities. For example, if an entity has the attributes calvedBy and SiredBy, we can infer the values of these two attributes relate two other entities as reproductive partners.
        So, for certain cases, you need to investigate the context and SDM to follow and understand the relationships between entities. In other cases, you might need to query some entities to receive more context and information to continue your reasoning. It might
        be hard in some cases, but a good rule of thumb is to always query the context file and follow certain links of the SDM.

        For the GET request, you should just send the query that will be send to the python package requests.
        Please, do not change any proper name. Some names might contain spaces, so you should use the %22 symbol to escape them.
        """
        #You should avoid retrieving all the entities of a type, because it is not efficient and it is not necessary.
        #When possible, use filters to retrieve only the entities that you need and use the sort, limit and offset parameters to retrieve only the entities that you need.

        try:
            openai_client = openai.OpenAI(api_key=openai.api_key)
            response = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_task}
            ]
            )
            logger.debug(f"created chat with OpenAI")
            dag_structure_str = response.choices[0].message.content
            logger.info(f"-----------------------------")
            logger.info(f"Generated DAG Structure: {dag_structure_str}")
            logger.info(f"-----------------------------")
            return json.loads(dag_structure_str)
        except Exception as e:
            logger.error(f"Error creating DAG plan: {e}")
            return {}

    def create_node_details(self, user_task: str, dag_nodes_structure: Dict[str, List[str]]) -> Dict[str, DAGNode]:
        """
        Populates the details for each node in the DAG.
        """
        # This would be another LLM call to define the goal for each node.
        # For this scaffold, we'll create placeholder nodes.
        return dag_nodes_structure

        logger.info("Populating DAG node details...")
        nodes = {}
        logger.info(f"***************")
        logger.info(f"dag_nodes_structure: {dag_nodes_structure}")
        logger.info(f"***************")

        for node_id in dag_nodes_structure.keys():
            logger.info(f"node_id: {node_id}")

            # In a real implementation, the goal would be generated by an LLM based on the node's position in the graph.
            goal = f"goal '{node_id}' related to task: {user_task}"
            nodes[node_id] = DAGNode(
                node_id=node_id,
                goal_description=goal,
                output_variable=f"output_{node_id}"
            )
        return nodes