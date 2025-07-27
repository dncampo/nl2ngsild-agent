from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DAGNode:
    """
    Represents a single node in the execution DAG.
    Each node corresponds to a specific sub-task.
    """
    def __init__(self, node_id: str, goal_description: str, output_variable: str, resources: List[str] = None):
        self.id: str = node_id
        self.goal_description: str = goal_description
        self.output_variable: str = output_variable  # Variable name to store the output
        self.resources: List[str] = resources if resources is not None else []
        self.output_data: Any = None
        self.error: str | None = None
        self.status: str = "pending"  # pending, running, completed, failed

    def to_dict(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the DAGNode for JSON serialization.
        """
        return {
            "id": self.id,
            "goal_description": self.goal_description,
            "output_variable": self.output_variable,
            "resources": self.resources,
            "output_data": self.output_data,
            "error": self.error,
            "status": self.status
        }
    def __iter__(self):
        """
        Allows the DAGNode to be converted to a dict for JSON serialization.
        """
        return iter(self.to_dict().items())

    def __getstate__(self):
        """
        Support for pickling and some JSON libraries.
        """
        return self.to_dict()


    def __json__(self):
        return self.to_dict()


    def __repr__(self):
        return f"DAGNode(id={self.id}, status={self.status}, goal='{self.goal_description}')"