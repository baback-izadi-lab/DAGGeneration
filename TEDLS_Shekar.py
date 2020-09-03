from data_representation import DAG
import numpy as np
import json
import pandas as pd


class EDLS:
    def __init__(self, dag_path):
        self.dag_path = dag_path

    def run(self, speed_setting, dls_algo=False):
        self.dag = DAG(self.dag_path, speed_setting=speed_setting)
        #print("printing speed_setting")
        # print(speed_setting)
        active_proc = sum([1 for i in speed_setting if i != None])
        """
        x = 0
        for i in speed_setting:
            if i == None:
                x += 1
        """
        self.speed_setting = speed_setting
        self.schedule = [[] for i in range(len(self.speed_setting))]
        self.remaining_tasks = set(self.dag.graph.nodes())
        self.origin_nodes = [
            x for x in self.dag.graph.nodes() if self.dag.graph.in_degree(x) == 0]
        self.ready_nodes = set(self.origin_nodes)
        self.assigned_proc = None
        self.assigned_node = None
        self.tfs = [0 for i in range(self.dag.graph.number_of_nodes())]

        self.current_proc_temps = [self.current_temp(
            0.4*14) for i in range(len(speed_setting))]
        #print(f'static levels : {self.dag.static_levels}')
        step = 0

        while len(self.remaining_tasks) > 0:
            all_edls = []
            self.ready_nodes = list(self.ready_nodes)
            for node in self.ready_nodes:
                dls = self.dl(node)
                if not dls_algo:
                    edls = dls + dls * (1 - self.normed_temp(node))
                    # print(
                    #    f'Task {node} : DLS= {dls}, (1 - normed_temp) = {dls * (1 - self.normed_temp(node))}')
                    edls[np.isnan(edls)] = np.NINF
                    all_edls.append(edls)
                else:
                    all_edls.append(dls)

            all_edls = np.array(all_edls)
            df = pd.DataFrame(data=all_edls,
                              index=self.ready_nodes,
                              columns=[i for i in range(len(self.speed_setting))])
            step += 1
            #print("Step {}".format(step))
            # print(df)

            node_index, self.assigned_proc = np.unravel_index(
                np.argmax(all_edls, axis=None), all_edls.shape)

            self.assigned_node = self.ready_nodes[node_index]

            # If highest node + processor comb has highest expected_temp
            # then, assign next highest TEDLS value for the same task

            assigned_expected_temp = self.expected_temp[self.assigned_proc]

            if (assigned_expected_temp == max(self.expected_temp)) & (active_proc > 1):
                #print(f'Assigned processor {self.assigned_proc} is too hot!')
                # print(f'{all_edls[node_index]}')
                # Next line gets the processor number of second highest TEDLS value
                self.assigned_proc = all_edls[node_index].argsort(
                )[-2:][::-1][-1]

                # print(
                #    f'Switching task {self.assigned_node} to processor {self.assigned_proc}')

            self.schedule[self.assigned_proc].append(self.assigned_node)

            # print("Task assigned: {}. To Processor: {}".format(
            #    self.assigned_node, self.assigned_proc))
            speed = self.speed_setting[self.assigned_proc]
            power = self.dag.data['proc_power'][str(
                self.assigned_proc)][speed][self.assigned_node]

            self.current_proc_temps[self.assigned_proc] = self.current_temp(
                power)

            #print(f'Current temperatures: {self.current_proc_temps}')
            # print('-------------------------------------------')
            # exec_time = self.dag.data['proc_exec'][str(
            #    self.assigned_proc)][self.speed_setting[self.assigned_proc]][node]

            self.ready_nodes = set(self.ready_nodes)
            self.ready_nodes.remove(self.assigned_node)
            self.remaining_tasks.remove(self.assigned_node)

            for n in list(self.dag.graph.successors(self.assigned_node)):
                # Check if any of the parents of the next task is unfinished
                n_ready = True
                for parent in list(self.dag.graph.predecessors(n)):
                    if parent in self.remaining_tasks:
                        n_ready = False
                        break
                if n_ready:
                    self.ready_nodes.add(n)

        return self.schedule

    def normed_temp(self, node):
        temps = []
        normed_temps = []
        self.expected_temp = []
        for proc, speed in enumerate(self.speed_setting):
            if speed is not None:
                power = self.dag.data['proc_power'][str(proc)][speed][node]
                self.expected_temp.append(self.current_temp(power))
                # temps.append(self.current_temp(power))
                temps.append(self.current_proc_temps[proc])
                #self.current_proc_temps[proc] = self.current_temp(power)
            else:
                # self.current_proc_temps.append(0)
                self.expected_temp.append(0)
                temps.append(0)
        # max_temps = [self.current_temp(14),
        #             self.current_temp(72),
        #             self.current_temp(205)]
        # correction
        # max_temps = [self.current_temp(205)
        #             for i in range(len(self.speed_setting))]
        max_temps = max(self.expected_temp)
        for i, temp in enumerate(self.expected_temp):
            if temp > 0:
                # normed_temps.append(temp/max_temps[i])
                normed_temps.append(temp/max_temps)
            elif temp == 0:
                normed_temps.append(np.Inf)
        return np.array(normed_temps)

    def current_temp(self, power):
        return power*0.249 + 320.15
        # return power * 0.249 + 47

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
                alphas.append(np.Inf)
        return np.array(alphas)

    def dl(self, node):
        dls = []
        for proc, speed in enumerate(self.speed_setting):
            if speed is not None:
                sl = self.dag.static_levels[node]
                da = self.data_ready(node, proc, speed)
                tf = self.proc_ready(node, proc, speed)
                # print("sl: {}, da:{}, tf:{}".format(sl, da, tf))
                dls_value = sl - max([da, tf]) + self.delta(node, proc, speed)

                dls.append(dls_value)
            else:
                dls.append(np.NINF)

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

    def get_agent_schedule(self):
        agent_schedule = {}
        for node in self.dag.graph.nodes():
            agent_data = {}
            agent_data['children'] = set([
                n for n in self.dag.graph.successors(node)])
            agent_data['parents'] = set(
                [n for n in self.dag.graph.predecessors(node)])

            for proc, nodes in enumerate(self.schedule):
                if node in nodes:
                    node_idx = nodes.index(node)
                    if node_idx < len(nodes)-1:
                        agent_data['children'].add(nodes[node_idx+1])
                    if node_idx > 0:
                        agent_data['parents'].add(nodes[node_idx-1])
                    agent_data['children'] = list(agent_data['children'])
                    agent_data['parents'] = list(agent_data['parents'])
                    agent_data['processor'] = proc
                    agent_data['speed'] = self.speed_setting[proc]
            agent_schedule[node] = agent_data
        return agent_schedule


if __name__ == "__main__":
    edls = EDLS('./results/DAG25/task_data.json')
    processor_speeds = [0, 0, 0, None, None]
    # Note if you want to run DLS algorithm uncoment following command
    schedule = edls.run(processor_speeds, dls_algo=False)
    #print('Final Schedule:')
    # print(schedule)
    agent_schedule = edls.get_agent_schedule()
    # print(schedule)
    json.dump(agent_schedule, open('./results/agent_schedule.json', 'w'))
