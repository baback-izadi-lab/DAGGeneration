from ..utils.data_representation import DAG
import numpy as np
import json
import pandas as pd
from ..utils.agent_schedule_runner import *
from ..utils.plot import plot


class EDLS:
    def __init__(self, dag_path, base_powers):
        self.dag_path = dag_path
        self.base_powers = base_powers

    def run(self, speed_setting, dls_algo=False):
        self.dag = DAG(self.dag_path, speed_setting=speed_setting)
        self.speed_setting = speed_setting
        self.schedule = [[] for i in range(len(self.speed_setting))]
        self.remaining_tasks = set(self.dag.graph.nodes())
        self.origin_nodes = [
            x for x in self.dag.graph.nodes() if self.dag.graph.in_degree(x) == 0]
        self.ready_nodes = set(self.origin_nodes)
        self.assigned_proc = None
        self.assigned_node = None
        self.tfs = [0 for i in range(self.dag.graph.number_of_nodes())]
        step = 0
        self.idle_energies = []
        while len(self.remaining_tasks) > 0:
            if len(self.remaining_tasks) < len(self.dag.graph.nodes()):
                # ------------------------------Experimental_____________________

                agent_schedule = self.get_agent_schedule()
                # print(agent_schedule)
                # with open('./results/agent_schedule.json', 'w') as f:
                #    json.dump(agent_schedule, f)

                runner = ScheduleRunner(schedule=agent_schedule,
                                        dag_data=self.dag.data,
                                        speed_setting=[0, 0, 0, None, None],
                                        base_powers=self.base_powers,
                                        beta=1.0,
                                        agent_system=False)

                runner.start()
                """
                print(f'Processor times: {runner.processor_times}')
                print(f'Total Task Energy: {runner.task_energy}')
                print(f'Makespan: {runner.max_time}')
                print(f'Idle Times: {runner.idle_times}')
                print(f'Idle Energy: {runner.idle_energy}')
                """
                self.idle_energies = runner.idle_energy
                self.task_energies = runner.task_energies
                # print(runner.task_energies)
                # ------------------------------Experimental_____________________

            all_edls = []
            self.ready_nodes = list(self.ready_nodes)
            for node in self.ready_nodes:
                dls = self.dl(node)
                if not dls_algo:
                    edls = dls + dls * (1 - self.alpha(node))
                    # edls = dls * (1 - self.alpha2(node))
                    # edls = dls/self.alpha(node)
                    edls[np.isnan(edls)] = np.NINF
                    all_edls.append(edls)
                else:
                    all_edls.append(dls)

            all_edls = np.array(all_edls)
            df = pd.DataFrame(data=all_edls,
                              index=self.ready_nodes,
                              columns=[i for i in range(len(self.speed_setting))])
            step += 1
            print("Step {}".format(step))
            print(df)

            node_index, self.assigned_proc = np.unravel_index(
                np.argmax(all_edls, axis=None), all_edls.shape)
            node = self.ready_nodes[node_index]
            self.schedule[self.assigned_proc].append(node)

            self.assigned_node = node

            print("Task assigned: {}. To Processor: {}".format(
                self.assigned_node, self.assigned_proc))
            print('-------------------------------------------')
            exec_time = self.dag.data['proc_exec'][str(
                self.assigned_proc)][self.speed_setting[self.assigned_proc]][node]

            self.ready_nodes = set(self.ready_nodes)
            self.ready_nodes.remove(node)
            self.remaining_tasks.remove(node)

            for n in list(self.dag.graph.successors(node)):
                # Check if any of the parents of the next task is unfinished
                n_ready = True
                for parent in list(self.dag.graph.predecessors(n)):
                    if parent in self.remaining_tasks:
                        n_ready = False
                        break
                if n_ready:
                    self.ready_nodes.add(n)

        return self.schedule

    def alpha(self, node):
        alphas = []
        energies = []
        for proc, speed in enumerate(self.speed_setting):
            if speed is not None:
                power = self.dag.data['proc_power'][str(proc)][speed][node]
                exec_time = self.dag.data['proc_exec'][str(proc)][speed][node]
                energy = power*exec_time
                if len(self.idle_energies) > 0:
                    energy = self.task_energies[proc] - \
                        self.idle_energies[proc] + power*exec_time

                energies.append(energy)
            else:
                energies.append(None)
        energies = np.array(energies, dtype=np.float64)
        max_energy = np.nanmax(energies)
        for energy in energies:
            if energy is None:
                alphas.append(np.Inf)
            else:
                alphas.append(energy/max_energy)
        # print(alphas)
        return np.array(alphas)

    def alpha2(self, node):
        alphas = []
        alphas2 = []
        energies = []
        idle_energies = []
        for proc, speed in enumerate(self.speed_setting):
            if speed is not None:
                power = self.dag.data['proc_power'][str(proc)][speed][node]
                exec_time = self.dag.data['proc_exec'][str(proc)][speed][node]
                energy = power*exec_time
                idle_energy = 0
                if len(self.idle_energies) > 0:
                    #energy -= self.idle_energies[proc]
                    energy = self.task_energies[proc] + power*exec_time
                    idle_energy = self.idle_energies[proc]
                energies.append(energy)
                idle_energies.append(idle_energy)
            else:
                energies.append(None)
                idle_energies.append(None)
        energies = np.array(energies, dtype=np.float64)
        max_energy = np.nanmax(energies)

        idle_energies = np.array(idle_energies, dtype=np.float64)
        max_idle_energy = np.nanmax(idle_energies)

        for energy in energies:
            if energy is None:
                alphas.append(np.Inf)
            else:
                alphas.append(energy/max_energy)

        for idle_en in idle_energies:
            if idle_en is None:
                alphas2.append(np.Inf)
            else:
                alphas2.append(idle_en/max_idle_energy)

        alphas = np.array(alphas)
        alphas2 = np.array(alphas2)
        return alphas - alphas2

    def dl(self, node):
        dls = []
        for proc, speed in enumerate(self.speed_setting):
            if speed is not None:
                sl = self.dag.static_levels[node]
                da = self.data_ready(node, proc, speed)
                tf = self.proc_ready(node, proc, speed)
                #print("sl: {}, da:{}, tf:{}".format(sl, da, tf))
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

            if 'processor' in agent_data:
                agent_schedule[node] = agent_data
        return agent_schedule


if __name__ == "__main__":
    p = 0.4
    base_powers = [85*p, 130*p, 205*p]
    edls = EDLS('./graph_gen/task.json', base_powers=base_powers)
    processor_speeds = [0, 0, 0]
    # Note if you want to run DLS algorithm uncoment following command
    schedule = edls.run(processor_speeds, dls_algo=False)
    agent_schedule = edls.get_agent_schedule()
    print(schedule)
    #json.dump(agent_schedule, open('./results/agent_schedule.json', 'w'))
    print(agent_schedule)
    with open('./results/agent_schedule.json', 'w') as f:
        json.dump(agent_schedule, f)

    runner = ScheduleRunner(schedule=agent_schedule,
                            dag_data=edls.dag.data,
                            speed_setting=processor_speeds,
                            base_powers=edls.base_powers,
                            beta=1.0,
                            agent_system=False)
    runner.start()
    # print(runner.processor_times)
    print(f'Makespan: {runner.max_time}')
    print(f'Total Energy: {runner.task_energy + sum(runner.idle_energy)}')
    plot(runner.processor_times[:3], runner.max_time, 'edls_mod_gannt.png')

    # ------------ FOr PEFT
    """
    agent_schedule = json.load(open('./results/agent_schedule_peft.json'))
    keys = list(agent_schedule.keys())
    for task in keys:
        agent_schedule[int(task)] = agent_schedule[task]
    runner = ScheduleRunner(schedule=agent_schedule,
                            dag_data=edls.dag.data,
                            speed_setting=processor_speeds,
                            base_powers=edls.base_powers,
                            beta=1.0,
                            agent_system=False)
    runner.start()
    # print(runner.processor_times)
    print(f'Makespan: {runner.max_time}')
    print(f'Total Energy: {runner.task_energy + sum(runner.idle_energy)}')
    plot(runner.processor_times[:3], runner.max_time, 'peft_gantt.png')
    """