import networkx as nx
import matplotlib.pyplot as plt

from spark_lineage.LineageParser import LineageParser
from spark_lineage.LineagerWrapper import LineageWrapper

class Lineage:
    def __init__(self, lineage_wrapper: LineageWrapper, produce_parser: LineageParser, require_parser: LineageParser):
        self.name = lineage_wrapper.func.__name__
        self.dataframe = lineage_wrapper.dataframe
        self.lineage = lineage_wrapper.lineage
        self.description = lineage_wrapper.description
        self.produce = produce_parser(lineage_wrapper).parse()
        self.require_parser = require_parser(lineage_wrapper).parse()

    def graph(self, nodes=[], edges=[]):
        nodes.append(self.name)
        for r in self.lineage:
            edges.append((self.name, r.name))
            r.graph(nodes, edges)
        return nodes, edges

    def print_graph(self):
        nodes, edges = self.graph()
        G = nx.Graph()
        G.add_edges_from(edges)
        G.add_nodes_from(nodes)
        
        nx.draw(
            G, 
            with_labels=True
            )

        pos=nx.shell_layout(G)
        x_values, y_values = zip(*pos.values())
        x_max = max(x_values)
        x_min = min(x_values)
        x_margin = (x_max - x_min) * 2
        plt.xlim(x_min - x_margin, x_max + x_margin)
        plt.savefig('foo.png')