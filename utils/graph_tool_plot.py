from graph_tool.all import *
import json

g = Graph()

vertices = [g.add_vertex() for i in range(25)]
edges = []

input_file = './results/TEDLS-NB/DAG-25/3_proc/task_data.json'
data = json.load(open(input_file))
arcs = data['arcs']

for arc in arcs:
    source, dest, wt = arcs[arc]
    edges.append(g.add_edge(source, dest))

pos = radial_tree_layout(g, g.vertex(0))

graph_draw(g, pos=pos, vertex_text=g.vertex_index, vertex_font_size=18,
           output_size=(800, 600), output="two-nodes.png")
