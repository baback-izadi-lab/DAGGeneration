from ..utils.SimulationData import *
from ..utils.CombGen import *
from copy import deepcopy
import math
import os


class StaticAlgorithm:
    """This class describes the static algorithm"""

    def edls(self, g, pc, gamma):
        """This is the main function that uses the rest of the functions
            described in this file Use only this function to evaluate the EDLS value
            Usage : sa = StaticAlgorithm()
            g  = sa.edls(g,pc,gamma)"""

        g = self.median(g, pc)
        g = self.delta(g, pc)
        g = self.static_level(g, pc)
        g = self.init(g)
        # self.print_median(g, pc)
        self.write_median(g)

        for i in g.task_order:
            g.current_task = i
            g = self.energy(g,pc)
            g = self.dynamic_level(g,pc,gamma)
            self.write_dl(g,pc,i)
            g=self.assign(g,pc)
            if i != g.task_order[len(g.task_order)-1]:
                g = self.next_ready(g,pc)
                g = self.next_da(g,pc)
                g = self.next_tf(g,pc)


        """    
        for i in range(g.num_tasks):
            g = self.energy(g, pc)
            g = self.dynamic_level(g, pc, gamma)
            # self.show_dl(g, pc, i)
            self.write_dl(g, pc, i)
            g = self.assign(g, pc)
            g = self.next_ready(g, pc)
            g = self.next_da(g, pc)
            g = self.next_tf(g, pc)
        """

        #self.show_results(g, pc)
        self.total_energy(g, pc)
        self.calc_exec_2(g, pc)
        self.write_results(g, pc)
        # print "List of processors %i" %(len(g.processors.queue))
        # print "List of processors %i" %(len(g.processors[pc.proc[g.hi_dl_proc]].queue))

        return g

    def median(self, g, pc):
        """The median for  each task is calculated
            pass the Digraph g and ProcComb pc"""

        # @type g digraph
        # @type pc proc_comb

        for task in range(g.num_tasks):
            median_array = []
            for proc in range(pc.num_proc):
                median_array.append(g.processors[pc.proc[proc]].exec_time[pc.speed[proc]][task])
            # median_array = sorted(median_array)
            median_array.sort()
            if (len(median_array) % 2 == 0):
                floor = len(median_array) / 2
                ceil = (len(median_array) / 2) - 1
                # print(floor,ceil,len(median_array)/2,len(median_array)/2)
                g.tasks[task].median = (median_array[int(floor)] + median_array[int(ceil)]) / 2
            else:
                g.tasks[task].median = median_array[int(len(median_array) / 2)]
            # del median_array[:]

        return g

    def delta(self, g, pc):
        """Calculates the value of delta i.e. the difference between the speeds of processors"""
        # @type g digraph
        # @type pc proc_comb
        for task in g.task_order:
            for proc in range(pc.num_proc):
                g.processors[pc.proc[proc]].delta[task] = (g.tasks[task].median - g.processors[pc.proc[proc]].exec_time[pc.speed[proc]][task])

        return g

    def static_level(self, g, pc):
        """Calculates static level of each task"""
        # @type g digraph
        # @type pc proc_comb
        for i in range(g.num_tasks):
            if (g.has_next_link(i) == 0):
                g.tasks[i].visited = 0
                g.tasks[i].SL = g.tasks[i].median
                for q in range(g.num_tasks):
                    if ((g.has_link(q, i) == 1)):
                        if (g.tasks[q].SL < g.tasks[q].median + g.tasks[i].SL):
                            g.tasks[q].SL = g.tasks[q].median + g.tasks[i].SL
                        # print "For task %i, SL = %f + %f" %(q, g.tasks[q].median,g.tasks[i].SL)
                        self.explore(q, g)

        return g

    def explore(self, currentNode, g):
        """This function is used in static_level"""
        # @type g digraph
        g.tasks[currentNode].visited = 0
        for q in range(g.num_tasks):
            if (g.has_link(q, currentNode)):
                if (g.tasks[q].SL < g.tasks[q].median + g.tasks[currentNode].SL):
                    g.tasks[q].SL = g.tasks[q].median + g.tasks[currentNode].SL
                self.explore(q, g)

        return g

    def init(self, g):
        """Initializes and sets up the iterations"""
        for i in range(g.num_tasks):
            # @type g digraph
            g.tasks[i].dependent = g.in_degree(i)
            if (g.tasks[i].dependent == 0):
                g.tasks[i].ready = 1
            else:
                g.tasks[i].ready = 0
        return g

    def energy(self, g, pc):
        """Calculates the energy for the ready tasks
            in each iteration"""

        # @type g digraph
        # @type pc proc_comb
        for task in range(g.num_tasks):
            if (g.tasks[task].ready == 1):
                maxEnergy = 0
                for proc in range(pc.num_proc):
                    g.processors[pc.proc[proc]].alpha[task] = (g.processors[pc.proc[proc]].power[pc.speed[proc]][task] *
                                                               g.processors[pc.proc[proc]].exec_time[pc.speed[proc]][
                                                                   task])
                    if (g.processors[pc.proc[proc]].alpha[task] > maxEnergy):
                        maxEnergy = g.processors[pc.proc[proc]].alpha[task]
                for proc in range(pc.num_proc):
                    g.processors[pc.proc[proc]].alpha[task] = g.processors[pc.proc[proc]].alpha[task] / maxEnergy
        return g

    def print_alpha(self, g, pc):
        """Calculates the value of alpha"""
        # @type g digraph
        # @type pc proc_comb
        for task in range(g.num_tasks):
            print(task)
            for proc in range(pc.num_proc):
                print(proc)
                print(len(g.processors[pc.proc[proc]].alpha))

    def dynamic_level(self, g, pc, gamma):
        """Calculates the dynamic level for ready tasks. Needs valid gamma value"""
        # @type g digraph
        # @type pc proc_comb
        for task in range(g.num_tasks):
            if (g.tasks[task].ready == 1):
                for proc in range(pc.num_proc):
                    # print("da = ",g.processors[pc.proc[proc]].DA[task])
                    DL = g.tasks[task].SL - max(g.processors[pc.proc[proc]].DA[task], g.processors[pc.proc[proc]].TF[task]) + g.processors[pc.proc[proc]].delta[task]
                    g.processors[pc.proc[proc]].Dyn[task] = gamma * (DL * (1 - g.processors[pc.proc[proc]].alpha[task]))
                    g.processors[pc.proc[proc]].DL[task] = DL + gamma * (DL * (1 - g.processors[pc.proc[proc]].alpha[task]))
        return g

    def assign(self, g, pc):
        """After calculation of dynamic level this method selects the processor-task combo
        with highest DL"""

        parent_exec_time = 0
        parent_task = g.parent[g.current_task]
        task_Start_time = []
        for proc in pc.proc:
            if len(g.processors[pc.proc.index(proc)].queue)>0:
                prev_exec_time_proc=g.processors[proc].exec_time[pc.speed[pc.proc.index(proc)]][len(g.processors[pc.proc.index(proc)].queue) - 1]
                if parent_task in g.processors[proc].queue:
                    parent_exec_time = g.task_exec_time[parent_task]
                else:
                    parent_exec_time = g.task_exec_time[parent_task] + g.weight[parent_task][g.current_task]
                task_Start_time.append(max(parent_exec_time,prev_exec_time_proc,g.processors[proc].currentTime))

            else:
                prev_exec_time_proc = 0
                parent_exec_time = 0
                if parent_task in g.processors[proc].queue:
                    parent_exec_time = g.task_exec_time[parent_task]
                else:
                    if parent_task is not None:
                        parent_exec_time = g.task_exec_time[parent_task] + g.weight[parent_task][g.current_task]
                task_Start_time.append(max(parent_exec_time, prev_exec_time_proc, g.processors[proc].currentTime))




        g.hi_dl = 0.0
        g.hi_dl_task = 0
        g.hi_dl_proc = 0

        dictDL = {}
        for proc in pc.proc:
            dictDL[proc] = g.processors[proc].DL[g.current_task]

        if parent_task is not None:
            siblings = [i for i, val in enumerate(g.links[parent_task]) if val==1]
            siblings.remove(g.current_task)
            for proc in pc.proc:
                for s in siblings:
                    if s in g.processors[proc].queue:
                        if proc in dictDL:
                            del dictDL[proc]

        for proc in dictDL:
            if (g.hi_dl<dictDL[proc]):
                g.hi_dl = dictDL[proc]
                g.hi_dl_task = g.current_task
                g.hi_dl_proc = proc

        start_time = task_Start_time[pc.proc.index(g.hi_dl_proc)]
        end_time = g.processors[g.hi_dl_proc].exec_time[pc.speed[pc.proc.index(g.hi_dl_proc)]][g.current_task]

        g.task_start_time[g.current_task] = start_time
        g.task_end_time[g.current_task] = start_time + end_time

        task = g.hi_dl_task

        for proc in pc.proc:
            if(proc==g.hi_dl_proc):
                idle_time = task_Start_time[pc.proc.index(proc)] - g.processors[proc].currentTime

                if idle_time>0 and len(g.processors[pc.proc.index(proc)].queue)>0:
                    if g.current_task != 0:
                        #Calculate for idle task
                        P_np = g.processors[proc].power[pc.speed[pc.proc.index(proc)]][g.num_tasks]
                        exec_time_np = idle_time

                else:
                    P_np = g.processors[proc].power[pc.speed[pc.proc.index(proc)]][task]
                    exec_time_np = g.processors[proc].exec_time[pc.speed[pc.proc.index(proc)]][task]

        g.processors[g.hi_dl_proc].currentTime = start_time + end_time
        g.task_exec_time[g.current_task] = start_time + end_time
        g.processors[g.hi_dl_proc].queue.append(g.hi_dl_task)
        print()
        g.processors[g.hi_dl_task].assigned_proc = g.hi_dl_proc
        g.scheduled_tasks.append(g.hi_dl_task)

        """
        for task in range(g.num_tasks):
            if (g.tasks[task].ready == 1):
                for proc in range(pc.num_proc):
                    if (g.hi_dl < g.processors[pc.proc[proc]].DL[task]):
                        g.hi_dl = g.processors[pc.proc[proc]].DL[task]
                        g.hi_dl_task = task
                        g.hi_dl_proc = proc
        g.processors[pc.proc[g.hi_dl_proc]].queue.append(g.hi_dl_task)
        g.tasks[g.hi_dl_task].assigned_proc = g.hi_dl_proc
        """


        # print "List of scheduled tasks has a length of %i" %(len(g.scheduled_tasks))
        # print "List of total tasks%i" %(len(g.tasks))

        g.tasks[g.hi_dl_task].ready = 0
        return g

    def next_tf(self, g, pc):
        """Calculates TF for the next iteration"""
        # @type g digraph
        # @type pc proc_comb
        for task in range(g.num_tasks):
            if (g.tasks[task].ready == 1):
                for proc in range(pc.num_proc):
                    if (g.hi_dl_proc != proc and g.processors[pc.proc[proc]].TF[task] != 0):
                        g.processors[pc.proc[proc]].TF[task] -= g.processors[pc.proc[g.hi_dl_proc]].exec_time[0][
                            g.hi_dl_task]
                        # if(g.processors[pc.proc[proc]].TF[task]<0):
                        #   g.processors[pc.proc[proc]].TF[task]=0
                    elif (g.hi_dl_proc == proc):
                        g.processors[pc.proc[proc]].TF[task] = g.processors[pc.proc[g.hi_dl_proc]].exec_time[0][
                            g.hi_dl_task]
        return g

    def next_da(self, g, pc):
        """Calculates the DA for the next iteration"""
        # @type g digraph
        # @type pc proc_comb
        for proc in range(pc.num_proc):
            if (g.processors[pc.proc[proc]].DA[g.current_task] != 0):
                g.processors[pc.proc[proc]].DA[g.current_task] -= g.processors[pc.proc[proc]].DA[g.current_task] - g.processors[pc.proc[g.hi_dl_proc]].exec_time[0][g.hi_dl_task]

        for task in g.task_order:
            if (g.tasks[task].ready == 1 and g.has_link(g.hi_dl_task, task)):
                for proc in pc.proc:
                    if (proc != g.hi_dl_proc):
                        g.processors[pc.proc[proc]].DA[task] = g.processors[pc.proc[g.hi_dl_proc]].exec_time[0][g.hi_dl_task] + g.weight[g.hi_dl_task][task]

                    if (proc == g.hi_dl_proc):
                        g.processors[pc.proc[proc]].DA[task] = g.processors[pc.proc[g.hi_dl_proc]].exec_time[0][g.hi_dl_task]

        return g

    def next_ready(self, g, pc):
        """Marks the tasks which are ready for execution in the next iteration"""
        # @type g digraph
        # @type pc proc_comb
        next_tasks = g.next_task(g.hi_dl_task)
        if next_tasks:
            for task in range(len(next_tasks)):
                g.tasks[next_tasks[task]].dependent = g.tasks[next_tasks[task]].dependent - 1
                if (g.tasks[next_tasks[task]].dependent == 0):
                    # print "Making task %i ready" %(next_tasks[task])
                    g.tasks[next_tasks[task]].ready = 1
        return g

    def total_energy(self, g, pc):
        """Calculates the value of the total energy consumed because of the schedule"""
        # @type pc proc_comb
        # @type g digraph

        for proc in range(pc.num_proc):
            for task in (g.processors[pc.proc[proc]].queue):
                g.total_energy += g.processors[proc].exec_time[pc.speed[proc]][task] * g.processors[proc].power[pc.speed[proc]][task]

    # print "Total energy calculated = " + str(g.total_energy)

    # The following functions are used for output only.
    # Functions with 'show' in its name displays data
    # Functions with 'write' in its name are used to write to a file
    def show_results(self, g, pc):
        """Prints task queue for each processor"""
        # @type pc proc_comb
        for proc in range(pc.num_proc):
            # print ""
            print(g.processors[pc.proc[proc]].queue)

    def show_dl(self, g, pc, i):
        """Prints DL for ready tasks at every iteration"""
        print
        "Iteration number : %i \n" % (i)
        print
        "Task     SL      DA      TF      delta       alpha       DL"
        # @type pc proc_comb
        for proc in range(pc.num_proc):
            # @type g digraph
            for task in range(g.num_tasks):
                if (g.tasks[task].ready == 1):
                    print
                    "%i %f %f %f %f %f %f " % (
                    task, g.tasks[task].SL, g.processors[pc.proc[proc]].DA[task], g.processors[pc.proc[proc]].TF[task],
                    g.processors[pc.proc[proc]].delta[task], g.processors[pc.proc[proc]].alpha[task],
                    g.processors[pc.proc[proc]].DL[task])
                    print
                    "%f" % (g.processors[pc.proc[proc]].Dyn[task])

    def print_median(self, g, pc):
        """Prints median for tasks at every iteration"""
        for count in range(g.num_tasks):
            print
            "%i median = %f SL = %f" % (count, g.tasks[count].median, g.tasks[count].SL)

    def write_median(self, g):
        self.lines = []
        line = "Median and Static Levels \n"
        self.lines.append(line)
        for count in range(g.num_tasks):
            a = "Task %i --> Median = %f SL = %f\n" % (count, g.tasks[count].median, g.tasks[count].SL)
            self.lines.append(a)

    def write_dl(self, g, pc, i):
        line1 = "\n\nIteration number : %i\n" % (i)
        line2 = "Task     SL      DA      TF      delta       alpha       DL\n"
        self.lines.append(line1)
        self.lines.append(line2)

        # @type pc proc_comb
        for proc in range(pc.num_proc):
            # @type g digraph
            line = 'For Processor %i :\n' % (pc.proc[proc])
            self.lines.append(line)
            for task in range(g.num_tasks):
                if (g.tasks[task].ready == 1):
                    line3 = "%i %f %f %f %f %f %f\n" % (
                    task, g.tasks[task].SL, g.processors[pc.proc[proc]].DA[task], g.processors[pc.proc[proc]].TF[task],
                    g.processors[pc.proc[proc]].delta[task], g.processors[pc.proc[proc]].alpha[task],
                    g.processors[pc.proc[proc]].DL[task])
                    # line4 = "%f\n" %(g.processors[pc.proc[proc]].Dyn[task])
                    self.lines.append(line3)
                    # self.lines.append(line4)

    def write_results(self, g, pc):
        for proc in range(pc.num_proc):
            self.lines.append('Task Queue on Processor %i  ' % (pc.proc[proc]))
            if g.processors[pc.proc[proc]].queue:
                self.lines.append(str(g.processors[pc.proc[proc]].queue) + '\n')
            else:
                self.lines.append("[]")

    def write_to_file(self, path):
        """Writes the data to file pass the path to the data file"""
        p = path.split('/')
        q = p[-1].split('.')
        q[1] = 'res'
        q = '.'.join(q)
        p[-1] = q
        path = '/'.join(p)
        f = open(path, 'w')
        f.writelines(self.lines)

    def ff2pc(self, ff):
        """Converts the full form of processor speeds to pc(ProcComb)"""
        num_proc = 0
        avail_speeds = []
        avail_proc = []
        for i in range(len(ff)):
            if ff[i] != 0:
                num_proc += 1
                avail_speeds.append(ff[i] - 1)
                avail_proc.append(i)
        pc = ProcComb(num_proc)
        pc.proc = avail_proc
        pc.speed = avail_speeds
        return pc

    def calc_exec_2(self, g, pc):
        g.qcopy = [] * pc.num_proc #Deep copies for agent_system.py
        for p in range(pc.num_proc):
            g.qcopy.append(copy.deepcopy(g.processors[pc.proc[p]].queue))
        total_exec_time = 0
        print(pc.proc)
        for proc in pc.proc:
            if g.processors[proc].currentTime > total_exec_time:
                total_exec_time += g.processors[proc].currentTime
        g.total_exec_time = total_exec_time
        print("Printing total execution time")
        print(g.total_exec_time)

        """
        end_of_task = 0
        self.total_exec_time = 0
        current_proc_time = [()] * pc.num_proc
        ready_tasks = [None] * pc.num_proc
        old_task = [False] * pc.num_proc
        current_task = 0
        prev_task = 0
        g = self.init(g)
        for i in range(g.num_tasks):
            self.total_exec_time = self.total_exec_time + end_of_task
            for task in range(g.num_tasks):
                for p in range(pc.num_proc):
                    if g.processors[pc.proc[p]].queue:
                        if g.tasks[task].ready and g.processors[pc.proc[p]].queue[0] == task and current_proc_time[p] == ():
                            ready_tasks[p] = task
                            current_proc = p
                            old_task[p] = True
            for t in range(len(ready_tasks)):
                if ready_tasks[t] != None and old_task[t] == True:
                    current_proc_time[t] = g.processors[pc.proc[t]].exec_time[pc.speed[t]][ready_tasks[t]]
                    old_task[t] = False
                    prev_tasks = g.prev_node(ready_tasks[t])
                    if prev_tasks != None:
                        for prev_task in prev_tasks:
                            if g.tasks[prev_task].assigned_proc != g.tasks[ready_tasks[t]].assigned_proc:
                                current_proc_time[t] = current_proc_time[t] + g.weight[prev_task][ready_tasks[t]]


            current_task = ready_tasks[current_proc_time.index(end_of_task)]

            current_proc = g.tasks[current_task].assigned_proc

            a = g.processors[pc.proc[current_proc]].queue.pop(0)
            for time in range(len(current_proc_time)):
                if current_proc_time[time] != ():
                    current_proc_time[time] = current_proc_time[time] - end_of_task

                if current_proc_time[time] <= 0:
                    prev_task = current_task

            g.tasks[prev_task].ready = 0
            next_tasks = g.next_task(prev_task)
            if next_tasks:
                for task in range(len(next_tasks)):
                    g.tasks[next_tasks[task]].dependent = g.tasks[next_tasks[task]].dependent - 1
                    if (g.tasks[next_tasks[task]].dependent == 0):

                        g.tasks[next_tasks[task]].ready = 1
        """

    def findTaskOrder(self,g,temp_list,task_order):
        temp = []
        for i in range(len(temp_list)):
            for j in range(len(g.links[temp_list[i]])):
                if g.links[temp_list[i]][j] ==1:
                    if j in task_order:
                        task_order.remove(j)
                    if j not in temp:
                        temp.append(j)
        temp.sort() #why?
        return temp

if __name__ == "__main__":

    """
    data = TaskGraphData()
    dat = DatHandler()
    dat.set_path(path)
    data = dat.read_dat()
    tg = TGFFGenerator(data, 'example_case0')
    tg.write_file()
    tc = TGFFcommand('example_case0')

    g = Digraph(data.num_tasks, data.num_proc)
    tp = TGFFParser(data, path.split('/')[-1].split('.')[0], g)
    g = tp.parse_tgff()
    sa = StaticAlgorithm()
    pc = ProcComb(2)
    pc.set_proc(0,0)
    pc.set_proc(1,0)
    pc.set_proc(2,0)
    g = sa.edls(g, pc, 0.5)
    """