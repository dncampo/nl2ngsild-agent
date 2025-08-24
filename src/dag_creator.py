import json
import logging
from typing import Dict, List
import openai
import time
from dag_node import DAGNode

logger = logging.getLogger(__name__)

class DAGCreator:
    """
    This class is responsible for interacting with the LLM to create a Directed Acyclic Graph (DAG) plan based on the user's task.
    It uses the OpenAI API to create an assistant and a thread to store the conversation history.
    Creates an interface to interact with the LLM.
    """
    def __init__(self, openai_api_key: str):
        try:
            self.client = openai.OpenAI(api_key=openai_api_key)
            self.assistant = self.client.beta.assistants.create(
                name="DAG Creator",
                instructions="You are an AI planner that helps to create plans to query a FIWARE Context Broker. You design a DAG to solve the user's task. Each node of the DAG is a step to solve the user's task. Each node has a description, a boolean indicating if it is atomic or not and a query to the Context Broker.",
                model="gpt-4.1-mini"
            )
            self.thread = self.client.beta.threads.create()
            run = self.client.beta.threads.runs.create(
                thread_id=self.thread.id,
                assistant_id=self.assistant.id,
                instructions="Create a plan to retrieve the requested information from the user."
            )
            while run.status in ["queued", "in_progress"]:
                time.sleep(0.5)
                run = self.client.beta.threads.runs.retrieve(thread_id=self.thread.id, run_id=run.id)
                logger.info(f"Run status: {run.status}")

        except Exception as e:
            logger.error(f"Error creating OpenAI connection: {e}")
            raise e
        return

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
            logger.info(f"Ask user question to LLM")
            # Create a message with user's task
            thread_message = self.client.beta.threads.messages.create(
                thread_id=self.thread.id,
                role="user",
                content=prompt,
            )

            # Run the Assistant
            my_run = self.client.beta.threads.runs.create(
                thread_id=self.thread.id,
                assistant_id=self.assistant.id,
                instructions="Please, return the DAG structure in JSON format. Do not include any other text or comments nor characters. NO markdown please."
            )

            # Periodically retrieve the Run to check on its status to see if it has moved to completed
            while my_run.status in ["queued", "in_progress"]:
                keep_retrieving_run = self.client.beta.threads.runs.retrieve(
                    thread_id=self.thread.id,
                    run_id=my_run.id
                )
                logger.info(f"Run status: {keep_retrieving_run.status}")
                time.sleep(1)

                if keep_retrieving_run.status == "completed":
                    logger.info("Completed\n")

                    # Step 6: Retrieve the Messages added by the Assistant to the Thread
                    all_messages = self.client.beta.threads.messages.list(
                        thread_id=self.thread.id
                    )
                    logger.info("------------------------------------------------------------ \n")
                    logger.info(f"User: {thread_message.content[0].text.value}")
                    logger.info(f"Assistant: {all_messages.data[0].content[0].text.value}")
                    break
                elif keep_retrieving_run.status == "queued" or keep_retrieving_run.status == "in_progress":
                    pass
                else:
                    logger.info(f"Run status: {keep_retrieving_run.status}")
                    break
            return json.loads(all_messages.data[0].content[0].text.value)
        except Exception as e:
            logger.error(f"Error creating DAG plan: {e}")
            return {}


    def send_message_to_llm(self, message: str) -> str:
        """
        Sends a message to the LLM. Probably the result of a previous node, or a CB's response.
        """
        try:
            logger.info(f"Ask user question to LLM")
            # Create a message with user's task
            thread_message = self.client.beta.threads.messages.create(
                thread_id=self.thread.id,
                role="user",
                content=message,
            )

            # Run the Assistant
            my_run = self.client.beta.threads.runs.create(
                thread_id=self.thread.id,
                assistant_id=self.assistant.id,
                instructions="Please, update data if this is a response or execute the query to give the answer to the user if this is a step."
            )
            # Periodically retrieve the Run to check on its status to see if it has moved to completed
            while my_run.status in ["queued", "in_progress"]:
                keep_retrieving_run = self.client.beta.threads.runs.retrieve(
                    thread_id=self.thread.id,
                    run_id=my_run.id
                )
                logger.info(f"Run status: {keep_retrieving_run.status}")
                time.sleep(1)

                if keep_retrieving_run.status == "completed":
                    logger.info("Completed\n")

                    # Step 6: Retrieve the Messages added by the Assistant to the Thread
                    all_messages = self.client.beta.threads.messages.list(
                        thread_id=self.thread.id
                    )
                    break
                elif keep_retrieving_run.status == "queued" or keep_retrieving_run.status == "in_progress":
                    pass
                else:
                    logger.info(f"Run status: {keep_retrieving_run.status}")
                    break

            logger.info(f"Assistant response to the update: {all_messages.data[0].content[0].text.value}")
            return json.loads(all_messages.data[0].content[0].text.value)
        except Exception as e:
            logger.error(f"Error sending message to LLM: {e}")
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