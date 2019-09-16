from data_representation import DAG
import numpy as np


class EDLS:
    def __init__(self, dag_path):
        self.dag = DAG(dag_path)

    def run(self, speed_setting, dls_algo=False):
        self.speed_setting = speed_setting
        self.schedule = [[] for i in range(len(self.speed_setting))]
        self.remaining_tasks = set(self.dag.graph.nodes())
        self.ready_nodes = set([0])
        self.assigned_proc = None
        self.assigned_node = None
        self.tfs = [0 for i in range(self.dag.graph.number_of_nodes())]
        while len(self.remaining_tasks) > 0:
            all_edls = []
            self.ready_nodes = list(self.ready_nodes)
            for node in self.ready_nodes:
                dls = self.dl(node)
                if not dls_algo:
                    edls = dls + dls * (1 - self.alpha(node))
                    all_edls.append(edls)
                else:
                    all_edls.append(dls)

            all_edls = np.array(all_edls)
            print(self.ready_nodes)
            print(all_edls)

            node_index, self.assigned_proc = np.unravel_index(
                np.nanargmax(all_edls, axis=None), all_edls.shape)
            node = self.ready_nodes[node_index]
            self.schedule[self.assigned_proc].append(node)
            self.assigned_node = node

            self.ready_nodes = set(self.ready_nodes)
            self.ready_nodes.remove(node)
            for n in list(self.dag.graph.adj[node]):
                self.ready_nodes.add(n)

            self.remaining_tasks.remove(node)

        return self.schedule

    def alpha(self, node):
        alphas = []
        energies = []
        for proc, speed in enumerate(self.speed_setting):
            if speed is not None:
                power = self.dag.data['proc_power'][str(proc)][speed][node]
                exec_time = self.dag.data['proc_exec'][str(proc)][speed][node]
                energies.append(power*exec_time)
            else:
                energies.append(0)
        max_energy = max(energies)
        for energy in energies:
            if energy > 0:
                alphas.append(energy/max_energy)
            elif energy == 0:
                alphas.append(float('inf'))
        return np.array(alphas)

    def dl(self, node):
        dls = []
        for proc, speed in enumerate(self.speed_setting):
            if speed is not None:
                dls_value = self.dag.static_levels[node] - max(
                    [self.data_ready(node, proc, speed), self.proc_ready(node, proc, speed)]) + self.delta(node, proc, speed)

                dls.append(dls_value)
            else:
                dls.append(0)

        return np.array(dls)

    def data_ready(self, node, proc, speed):
        da = 0
        parent = list(self.dag.graph.predecessors(node))
        if parent:
            parent = parent[0]
            if parent == self.assigned_node:
                parent_assigned_proc = self.get_assigned_proc(parent)
                parent_speed = self.speed_setting[parent_assigned_proc]
                parent_exec_time = self.dag.data['proc_exec'][str(
                    parent_assigned_proc)][parent_speed][parent]
                da = parent_exec_time
                if parent_assigned_proc != proc:
                    da += self.dag.graph[parent][node]['weight']
        return da

    def proc_ready(self, node, proc, speed):

        tf = 0
        if self.assigned_proc:
            assigned_speed = self.speed_setting[self.assigned_proc]
            assigned_exec_time = self.dag.data['proc_exec'][str(
                self.assigned_proc)][assigned_speed][self.assigned_node]
            if self.assigned_proc != proc and self.tfs[node] != 0:
                self.tfs[node] -= assigned_exec_time
            elif self.assigned_proc == proc:
                self.tfs[node] = assigned_exec_time
        tf = self.tfs[node]
        return tf

    def delta(self, node, proc, speed):
        """ Calculates delta for a task at all speed settings """
        exec_time = self.dag.data['proc_exec'][str(proc)][speed][node]
        return self.dag.median_times[node] - exec_time

    def get_assigned_proc(self, node):
        for proc, nodes in enumerate(self.schedule):
            if node in nodes:
                return proc


if __name__ == "__main__":
    edls = EDLS('test_simple.json')
    processor_speeds = [None, 0, 0]
    # Note if you want to run DLS algorithm uncoment following command
    #schedule = edls.run(processor_speeds, dls_algo=True)
    schedule = edls.run(processor_speeds)
    print(schedule)
