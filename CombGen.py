import copy
from SimulationData import *

class TGFFParser:
    """This class is the final step in creation of the digraph
    It reads the TGFF file generated and stores the data into the TaskGraph class"""

    def __init__(self, data, tgff_filename, g):
        # print "I'm in TGFFParser __int__!"
        self.g = g
        self.data = data

        for proc in range(data.num_proc):
            p = Processor(data.get_num_speeds(proc), data.get_num_tasks())
            g.processors.append(p)

    def parse_tgff(self):
        """Reads the tgff file"""
        self.tgff_file = open('/home/rashad/Documents/tgff-3.6/examples/example_case0.tgff', 'r')
        text = self.tgff_file.readlines()
        lc = 0
        taskType = 50  # Total number of task types is always 50
        arcTypeVal = []

        for lines in text:
            line = lines.split()
            if ('@arcs' in line):
                for x in range(taskType):
                    arcTypeVal.append(float(text[lc + x + 6].split()[1]))
            lc += 1

        for lines in text:
            line = lines.split()
            if ('ARC' in line):
                for source in range(self.data.num_tasks):
                    for dest in range(self.data.num_tasks):
                        if (source == (int(line[3].split("_")[1]))) and (dest == (int(line[5].split("_")[1]))):
                            value = arcTypeVal[int(line[7])]
                            e = Edge(source, dest, value)
                            self.g.set_edge(e)
        # procs = [0,1,2]
        # procs = 3
        # print("Printing self.data.num_proc")
        # print(self.data.num_proc)
        for proc in range(self.data.num_proc):
            # range(procs):

            #print("working on processor execution: " + str(proc))
            lc = 0
            for lines in text:
                task = 0
                line = lines.split()
                if ('exec' + str(proc) + '0' in line):
                    for x in range(self.data.num_tasks):
                        for speed in range(self.data.get_num_speeds(proc)):
                            self.g.processors[proc].add_exec(float(text[lc + x + 1].split()[2 + speed]), speed, task)
                        task += 1
                lc += 1

            lc = 0
            for lines in text:
                task = 0
                line = lines.split()
                if ('power' + str(proc) + '0' in line):
                    for x in range(self.data.num_tasks):
                        for speed in range(self.data.get_num_speeds(proc)):
                            self.g.processors[proc].add_power(float(text[lc + x + 1].split()[2 + speed]), speed, task)
                        lastTask = task
                        task += 1
                lc += 1

            # add power for idle
            for speed in range(self.data.get_num_speeds(proc)):
                if speed == 0:
                    self.g.processors[proc].add_power(0.5,speed,lastTask)
                elif speed == 1:
                    self.g.processors[proc].add_power(1.5,speed,lastTask)
                else:
                    self.g.processors[proc].add_power(2.5,speed,lastTask)

        return self.g

class Task:
    """This class represents the information stored in each node"""

    def __init__(self, number):
        self.number = number
        self.SL = 0

class Edge:
    "stores info about edge"

    def __init__(self, source, dest, value):
        self.source = source
        self.dest = dest
        self.value = value
        # def disp(self):
        #print("Source =  %i  Dest =  %i  Value =  %d" )%(self.source,self.dest,self.value)

class Processor:
    "stores execution times and power consumed by the processor for each task"

    def __init__(self, speeds, tasks):
        self.speeds = speeds
        self.tasks = tasks
        self.exec_time = [[0 for count in range(tasks)] for count in range(speeds)]
        self.power = [[0 for count in range(tasks)] for count in range(speeds)]
        self.initial_temperature = [[0 for count in range(tasks)] for count in
                                    range(speeds)]  # array of initial temperature values during iterations
        self.final_temperature = [[0 for count in range(tasks)] for count in
                                  range(speeds)]  # array of final temperature values during iterations
        self.DA = [0] * tasks
        self.Dyn = [0] * tasks
        self.DL = [0] * tasks
        self.TF = [0] * tasks
        self.delta = [0] * tasks
        self.alpha = [0] * tasks
        self.temp = [0] * tasks  # can be changed to norm_temp later
        self.queue = []
        self.currentTime = 0

    def add_exec(self, execTime, speed, task):
        self.exec_time[speed][task] = execTime

    # print "Printing execution time"
    # print self.exec_time

    def add_power(self, power, speed, task):
        self.power[speed][task] = power

    # print self.power[speed][task]

class Digraph:
    """uses node, edge and processor classes in this class to create digraphs"""

    def __init__(self, tasks, procs):
        self.total_energy = 0
        self.total_exec_time = 0
        self.num_tasks = tasks
        self.num_processors = procs
        self.links = [[0 for count in range(tasks)] for count in range(tasks)]
        self.parent = [None for count in range(tasks)]
        self.weight = [[0 for count in range(tasks)] for count in range(tasks)]
        self.tasks = []  # array of tasks
        self.scheduled_tasks = []  # array of scheduled tasks
        self.processors = []  # array of processors
        self.task_queue = []  # queue of tasks
        self.num_edges = 0
        self.edge_list = []  # list of edges
        #self.temperature = [313.15 for count in range(procs)]  # array of temperature values
        #self.temperature_values = []
        self.task_order = []
        self.current_task = 0
        self.current_time = 0.0
        #self.ro = 0.0
        #self.beta = 0.0
        self.task_exec_time = [0 for count in range(tasks)]
        self.current_pc = 0
        self.task_start_time = [0 for count in range(tasks)]
        self.task_end_time = [0 for count in range(tasks)]
        # print "temperature = %s" %(self.temperature)
        for i in range(self.num_tasks):
            t = Task(i)
            self.tasks.append(t)
        #        print "total_energy"
        #        print self.total_energy
        #        print "tasks"
        #        print self.tasks
        #        print "num_processors"
        #        print self.num_processors
        #        print "links"
        #        print self.links
        #        print "weight"
        #        print self.weight

    def nodes(self):  # returns number of tasks
        return self.num_tasks

    def out_degree(self, task):  # returns number of tasks going out
        total = 0
        for count in range(self.num_tasks):
            if (self.links[task][count]):
                total += 1
        return total

    def in_degree(self, task):  # returns number of tasks coming in
        total = 0
        for count in range(self.num_tasks):
            if (self.links[count][task]):
                total += 1
        return total

    def has_prev_link(self, task):  # checks if node has prev nodes
        val = 0
        for count in range(self.num_tasks):
            if (self.links[count][task]):
                val = 1
        return val

    def has_next_link(self, task):
        val = 0
        for count in range(self.num_tasks):
            if (self.links[task][count]):
                val = 1
        return val

    def has_link(self, task1, task2):
        if (self.links[task1][task2]):
            return 1
        else:
            return 0

    def next_task(self, task):
        if (self.has_next_link(task)):
            # print "Task %i has link!" %(task)
            next = []
            for count in range(self.num_tasks):
                if (self.has_link(task, count)):
                    next.append(count)
            return next

    def prev_node(self, task):
        # print "has prev link value = " + str(self.has_prev_link(task))
        if (self.has_prev_link(task)):
            prev = []
            for count in range(self.num_tasks):
                #	print str(count) + " and " + str(task) + " has a link"
                if (self.has_link(count, task)):
                    prev.append(count)
            return prev

    def set_edge(self, edge):
        if (self.links[edge.source][edge.dest] == 0):
            self.parent[edge.dest] = edge.source
            self.links[edge.source][edge.dest] = 1
            self.weight[edge.source][edge.dest] = edge.value
            self.edge_list.append(edge)
            self.num_edges += 1
            #print "edge source: " + str(edge.source)

    def set_node(self, node):
        self.tasks.append(node)

    def set_proc(self, proc):
        self.processors.append(proc)

    def reset(self):
        self.total_energy = 0
        # self.total_exec_time = 0
        self.task_queue = []

    def getLastNodes(self):
        counter = 0
        for count in range(self.num_tasks):
            if self.out_degree(count)==0:
                counter += 1;

class ProcComb:
    "This class is used to select certain processors from a pool"

    def __init__(self, num_proc):
        self.num_proc = num_proc
        self.proc = []  # stores processor number in order
        self.speed = []  # stores corresponding speed number

    def set_proc(self, p_num, f_num):
        self.proc.append(p_num)
        self.speed.append(f_num)

class CombGen:
    """This class generates all possible combinations of processors and speeds
    Usage:  cg = CombGen(g)  --->(g is the Digraph class found in data.py)
                all_comb = cg.proc_comb() --->(Returns list of ProcComb of all combinations)
                cg.all_speeds ---> List of all combinations in the form [0, 1, 0,3] Where '0' represents
                                            processors not used and ints 1 and 3 are processor speeds
                                            Position of the integer denotes status of particular processor
                                            In the example Processors 0 and 2 are switched off"""

    def __init__(self, g):
        """Pass Digraph g to initialize. g should be ready for computation"""
        # @type self.g Digraph
        self.g = g
        self.p = []
        for i in range(g.num_processors):
            self.p.append(i)
        self.all_combinations = []
        self.all_speeds = []

    def combination(self, seq, length):
        """Algorithm used to generate different processor combinations"""
        if not length:
            return [[]]
        else:
            l = []
            for i in range(len(seq)):
                for result in self.combination(seq[i + 1:], length - 1):
                    l += [[seq[i]] + result]
            return l

    def proc_comb(self):
        """Algorithm used to generate processor comb as well as speeds"""
        for p in range(2, self.g.num_processors + 1):
            # @type pc ProcComb
            l = self.combination(self.p, p)
            # print l
            for comb in l:
                self.speed_comb(comb)
        return self.all_combinations

    def speed_comb(self, comb):
        """Generates the speed combinations for the processor combination passed to it
        in the list 'comb'"""
        speeds = [0] * len(self.g.processors)
        count = 0
        self.next_comb(count, comb, speeds)

    def next_comb(self, count, comb, speeds):
        """Recursive function used in function speed_comb"""
        if (count < len(comb)):
            for x in range(self.g.processors[comb[count]].speeds):
                speeds[comb[count]] = x + 1
                count += 1
                self.next_comb(count, comb, speeds)
                count -= 1
        if (count == len(comb)):
            # print 'speeds : ' + str(speeds)
            speed_cpy = copy.deepcopy(speeds)
            self.all_speeds.append(speed_cpy)
            pc = ProcComb(len(comb))
            pc.proc = comb
            for sp in pc.proc:
                pc.speed.append(speeds[sp] - 1)
            # print str(pc.proc) + '   ' + str(pc.speed)
            self.all_combinations.append(pc)


if __name__ == "__main__":
    print("This is Start of main")

    data = TaskGraphData()
    dat = DatHandler()
    dat.set_path('/home/rashad/PycharmProjects/DAGGeneration/example_case0.dat')
    data = dat.read_dat()
    tg = TGFFGenerator(data, 'example_case0')
    tg.write_file()
    tc = TGFFcommand('example_case0')

    g = Digraph(data.num_tasks, data.num_proc)
    tp = TGFFParser(data, 'example_case0', g)
    g = tp.parse_tgff()
    cg = CombGen(g)
    all_combinations = cg.proc_comb()

    print("This is End of main")