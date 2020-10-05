from .SimulationData import *
from .CombGen import *
from ..algorithms.EDLS import *
import copy
import math
import os


class ImplementationSA():

    def __init__(self, algo_name, int_gamma,path):
        self.data = TaskGraphData()
        dat = DatHandler()
        dat.set_path(path)
        self.data = dat.read_dat()

        self.g = Digraph(self.data.num_tasks,self.data.num_proc)
        self.tp = TGFFParser(self.data, path.split('/')[-1].split('.')[0], self.g)
        self.g = self.tp.parse_tgff()

        self.gamma = int_gamma
        p = [path]
        q = p[-1].split('.')
        q = [w.replace('dat', 'csv') for w in q]
        if not os.path.exists(q[0]):
            os.makedirs(q[0])
        if algo_name == "EDLS" and int_gamma == 0:
            q[0] = os.path.join(q[0], "DLS")
            strOutput = "Working on DLS Algorithm"
        else:
            q[0] = os.path.join(q[0], algo_name)
            strOutput = "Working on EDLS Algorithm"
        print(strOutput)
        q='.'.join(q)
        p[-1] = q
        self.path = '/'.join(p)
        cg = CombGen(self.g)
        all_combinations = cg.proc_comb()
        all_speeds = cg.all_speeds
        lines = []
        all_energies = []
        all_exec_time = []
        all_exec_No_sec = []

        #Calculate task order
        returnlist = []
        itr = 1
        task_order = [0]
        temp_list = [0]
        while len(temp_list) > 0:
            returnlist = self.findTaskOrder(self.g, temp_list, task_order)
            for iTask in returnlist:
                if iTask in task_order:
                    task_order.remove(iTask)
            temp_list = returnlist
            task_order = task_order + temp_list

        self.g.task_order = task_order

        #self.g_temp = copy.deepcopy(self.g)
        #self.tp_temp = copy.deepcopy(self.tp)

        for pc in all_combinations:
            #self.g = copy.deepcopy(self.g_temp)
            #self.tp = copy.deepcopy(self.tp_temp)
            self.g.current_pc = itr
            sa = StaticAlgorithm()
            self.g = sa.edls(self.g,pc,self.gamma)

            all_energies.append(("%.2f" % self.g.total_energy) + 'Joules')
            all_exec_time.append(str("%.2f" % (self.g.total_execution_time/1000)) + 'seconds')

            #prntString = ""
            itr = itr + 1
        lines.append('This file contains all the possible combination of processors with their consumed energy and execution time\n')
        lines.append('Speed of each processor is given in a row. "0" indicates and unused processor.\n\n')
        line = ''
        for j in range(self.g.num_processors):
            line += 'P' + str(j) + ','
        line += 'Energy and Execution time\n'
        lines.append(line)

        for j in range(len(all_speeds)):
            line = ','.join(["%s" % elem for elem in all_speeds[j]])+','+str(all_energies[j])+','+str(all_exec_time[j]) +'\n'
            lines.append(line)

        file = open(self.path,'w')
        file.writelines(lines)
        file.close()

    def findTaskOrder(self,g,temp_list,task_order):
        temp = []
        for i in range(len(temp_list)):
            for j in range(len(g.links[temp_list[i]])):
                if g.links[temp_list[i]][j] ==1:
                    if j in task_order:
                        task_order.remove(j)
                    if j not in temp:
                        temp.append(j)
        temp.sort()
        return temp

if __name__ == "__main__":
    path = "/home/rashad/PycharmProjects/DAGGeneration/example_case0.dat"
    ImplementationSA("EDLS", 1, path)
