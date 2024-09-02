from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any

app = FastAPI()

class Pipeline(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]

def is_dag(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> bool:
    from collections import defaultdict, deque

    # Build adjacency list
    adj_list = defaultdict(list)
    in_degree = defaultdict(int)
    
    for edge in edges:
        source = edge['source']
        target = edge['target']
        adj_list[source].append(target)
        in_degree[target] += 1
        if source not in in_degree:
            in_degree[source] = 0
    
    # Kahn's algorithm for detecting cycles in a graph
    zero_in_degree = deque([node for node in in_degree if in_degree[node] == 0])
    visited_count = 0

    while zero_in_degree:
        node = zero_in_degree.popleft()
        visited_count += 1
        for neighbor in adj_list[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                zero_in_degree.append(neighbor)

    return visited_count == len(in_degree)

@app.post('/pipelines/parse')
def parse_pipeline(pipeline: Pipeline):
    nodes = pipeline.nodes
    edges = pipeline.edges

    num_nodes = len(nodes)
    num_edges = len(edges)
    dag_status = is_dag(nodes, edges)

    return {'num_nodes': num_nodes, 'num_edges': num_edges, 'is_dag': dag_status}
