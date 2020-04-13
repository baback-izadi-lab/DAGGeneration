from data_representation import DAG
import numpy as np
import json
import pandas as pd
import math
from agent_schedule_runner import *


class EDLS:
    def __init__(self, dag_path):
        self.dag_path = dag_path

    # The output of the following method is "all_edls", which is a list of lists called "edls".
    def run(self, speed_setting, dls_algo):
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
        self.temp = [313.15 for i in speed_setting]
        self.potential_end_temp = []
        self.max_temp = 358.15
        print("Printing initial_temp", self.initial_temp)
        step = 0

        self.task_in_order = []
        runner = []
        while len(self.remaining_tasks) > 0:
            all_edls = []
            all_tedls = []
            task_in_order = self.add_node(self.task_in_order, self.ready_nodes)
            #print("Printing tasks in order: ")
            # print(self.task_in_order)
            self.ready_nodes = list(self.ready_nodes)
            for node in self.ready_nodes:
                # task_in_order.remove(node)
                #print("Printing ready node: ")
                # print(self.ready_nodes)
                dls = self.dl(node)
                if (dls_algo == 0):
                    print("DLS algorithm")
                    all_edls.append(dls)
                elif (dls_algo == 1):
                    print("EDLS algorithm")
                    edls = dls + dls * (1 - self.alpha(node))
                    edls[np.isnan(edls)] = np.NINF
                    all_edls.append(edls)
                    #agent_schedule = edls.get_agent_schedule()
                else:
                    #print("TEDLS algorithm")
                    proc = len(speed_setting)
                    # potential end temperatures for all processors using a heat model. problem with idle time.
                    # normalize temperature values to yield aplha like value
                    # All TEDLS values tedls -> edls
                    initial_temp = self.initial_temp(
                        node, proc, self.temp, runner)
                    norm_temp = self.norm_temp(node, initial_temp)
                    edls = dls + dls * (1 - norm_temp)
                    edls[np.isnan(edls)] = np.NINF
                    all_edls.append(edls)

            all_edls = np.array(all_edls)
            df = pd.DataFrame(data=all_edls, index=self.ready_nodes, columns=[
                              i for i in range(len(self.speed_setting))])
            step += 1

            node_index, self.assigned_proc = np.unravel_index(
                np.argmax(all_edls, axis=None), all_edls.shape)
            node = self.ready_nodes[node_index]
            self.schedule[self.assigned_proc].append(node)
            #print("Printing schedule: ", self.schedule)
            # assigning final temperature to temp
            self.temp[self.assigned_proc] = norm_temp[self.assigned_proc] * self.max_temp
            print("Printing temperature", self.temp)
            # Major updates
            agent_schedule = self.get_agent_schedule()
            new_agent_schedule = {}
            for x, y in agent_schedule.items():
                if(len(y) == 4):
                    new_agent_schedule[x] = agent_schedule[x]
            #print("Printing agent_schedule ", agent_schedule)
            #print("Printing new_agent_schedule ", new_agent_schedule)
            #dumped_value = json.dumps(agent_schedule)
            #print("Printing dumped_value ",dumped_value)

            json.dump(new_agent_schedule, open('agent_schedule.json', 'w'))
            base_powers = [5, 15, 25]
            runner = ScheduleRunner(schedule='agent_schedule.json',
                                    dag_data='./results/EDLS/DAG-10/task_data.json',
                                    # speed_setting=[None, None, 2],
                                    speed_setting=[0, 0, 0],
                                    base_powers=base_powers,
                                    agent_system=True)  # for every iteration 3
            runner.start()
            #print("Printing execution times\n",runner.processor_times)

            self.assigned_node = node
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
            #print("This is the speed setting",self.speed_setting)
        return self.schedule

    def add_node(self, a, b):
        result = False
        for x in b:
            for y in a:
                if x == y:
                    result = True
            if (result == False):
                a.append(x)
        return a

    def initial_temp(self, node, proc, temp, runner):
        # get temperature of proc
        tentative_initial_temp = []
        lengths = []
        for x in range(proc):
            if self.task_in_order == [0]:
                tentative_initial_temp.append(temp[x])

            else:
                length = len(runner.processor_times[x])
                end_times = []
                last_tasks = []
                for prc in range(proc):
                    # lengths.append(len(runner.processor_times[0]))
                    if runner.processor_times[prc] != 0:
                        end_times.append(runner.processor_times[0][-1][2])
                        last_tasks.append(runner.processor_times[0][-1][0])
                    else:
                        end_times.append(0)
                        last_tasks.append(None)

                #length1 = len(runner.processor_times[0])
                #length2 = len(runner.processor_times[1])
                #length3 = len(runner.processor_times[2])
                #b = proc0_end_time
                #task_b = proc0_last_task
                # c = proc1_end_time
                # task_c = proc1_last_task
                # d = proc2_end_time
                # task_d = proc2_last_task
                """
                if (length1 != 0):
                    proc0_end_time = runner.processor_times[0][length1-1][2]
                    proc0_last_task = runner.processor_times[0][length1 - 1][0]
                else:
                    proc0_end_time = 0
                    proc0_last_task = None
                if (length2 != 0):
                    proc1_end_time = runner.processor_times[1][length2 - 1][2]
                    proc1_last_task = runner.processor_times[1][length2 - 1][0]
                else:
                    proc1_end_time = 0
                    proc1_last_task = None
                if (length3 != 0):
                    proc2_end_time = runner.processor_times[2][length3 - 1][2]
                    proc2_last_task = runner.processor_times[2][length3 - 1][0]
                else:
                    proc2_end_time = 0
                    proc2_last_task = None
                """
                parent = list(self.dag.graph.predecessors(node))
                #print("Printing parent ", parent)

                # Add comm time to start_time and change parent[0]
                for prc in range(proc):
                    if parent[0] == last_tasks[prc] and x != prc:
                        start_time = end_times[prc]
                """
                if((parent[0] == proc0_last_task) and (x != 0)):
                    start_time = proc0_end_time  # plus the communication time
                elif((parent[0] == proc1_last_task) and (x != 1)):
                    start_time = proc1_end_time  # plus the communication time
                elif ((parent[0] == proc2_last_task) and (x != 1)):
                    start_time = proc2_end_time  # plus the communication time
                
                """
                # Not parent[0] but parent with highest exec time
                # Parent task not on current processor
                if (((parent[0] == last_tasks[0]) and (x != 0)) or
                    ((parent[0] == last_tasks[1]) and (x != 1)) or
                        ((parent[0] == last_tasks[2]) and (x != 2))):

                    if ((length != 0) and (start_time > runner.processor_times[x][-1][2]) and (temp[x] != 313.15)):
                        idle_time = start_time - \
                            runner.processor_times[x][-1][2]

                        beta = 1
                        ro = 0.009999
                        # Hard coded power
                        #power = 5*0.4
                        power = self.dag.data['proc_power'][str(
                            x)][0][node]*0.4

                        #first_term = (beta * power) / ro
                        # idle_temp = first_term + \
                        #    (((temp[x]) - first_term) *
                        #     (math.exp(-ro * idle_time)))
                        idle_temp = self.calc_temp(power)
                        # if(idle_temp <= 313.15):
                        #    idle_temp = 313.15
                        tentative_initial_temp.append(idle_temp)
                    else:
                        tentative_initial_temp.append(temp[x])
                else:
                    tentative_initial_temp.append(temp[x])

        return tentative_initial_temp

    def calc_temp(self, power):
        return power*0.249 + 320.15

    def norm_temp(self, node, initial_temp):
        tentative_norm_temp = []
        beta = 1
        ro = 0.009999
        for proc, speed in enumerate(self.speed_setting):
            power = self.dag.data['proc_power'][str(proc)][speed][node]
            exec_time = self.dag.data['proc_exec'][str(proc)][speed][node]

            # Insert temp equation here

            #first_term = (beta*power)/ro
            # final_temp = first_term + \
            #    ((initial_temp[proc]-first_term)*(math.exp(-ro*exec_time)))
            final_temp = self.calc_temp(power)
            final_temp = final_temp/self.max_temp
            tentative_norm_temp.append(final_temp)
        return np.array(tentative_norm_temp)

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
            #print("Printing proc", proc)
            #print("Printing speed", speed)
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
            agent_data['children'] = set(
                [n for n in self.dag.graph.successors(node)])
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
    edls = EDLS('./results/DAG25/DAG_25_5.json')
    processor_speeds = [0, 0, 0]
    # Note if you want to run DLS algorithm uncomment following command
    schedule = edls.run(processor_speeds, 2)
    #agent_schedule = edls.get_agent_schedule()
    print(schedule)
    # json.dump(agent_schedule, open('agent_schedule.json', 'w')) # for every iteration
