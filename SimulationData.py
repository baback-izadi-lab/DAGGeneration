import os

class TaskGraphData:
    """This class stores all data related processor scheduling which is written to file using DatHandler class"""
    def __init__(self):
        self.average_exec = {}
        self.variation_exec = {}
        self.average_power = {}
        self.variation_power = {}

    def set_num_proc(self, num_proc):
        """Sets the number of processors running the Task Graph"""
        self.num_proc = int(num_proc)

    def get_num_proc(self):
        """Returns number of processors running the Task Graph"""
        return self.num_proc

    def set_num_tasks(self, num_tasks):
        """Sets number of tasks"""
        self.num_tasks = int(num_tasks)

    def get_num_tasks(self):
        """Returns number of tasks"""
        return self.num_tasks

    def set_num_speeds(self, speeds):
        """Sets the number of speeds for every processor.
        "speeds" should be a list with number of speeds
        corresponding to processor num in list"""
        self.num_speeds = speeds
        """
        self.num_speeds = []
        for i in range(data.get_num_proc()):
            proc_speed = int(input("Please input number of speeds of processor " + str(i) + ": "))
            self.num_speeds.append(proc_speed)
        """

    def get_num_speeds(self, proc):
        """Get number of speeds for the processor"""
        return self.num_speeds[proc]

    def set_average_exec(self, proc, speed, exec_time):
        """Sets the average exectime for a particular
        proc-speed combination It is stored in dictionaries"""
        proc = str(proc)
        speed = str(speed)
        key = proc + speed
        self.average_exec[key] = float(exec_time)

    def get_average_exec(self, proc, speed):
        """Returns the average exec tme"""
        proc = str(proc)
        speed = str(speed)
        key = proc + speed
        return self.average_exec[key]

    def set_average_power(self, proc, speed, power):
        """Sets the average power for a particular
        proc-speed combination It is stored in dictionaries"""
        proc = str(proc)
        speed = str(speed)
        key = proc + speed
        self.average_power[key] = float(power)

    def get_average_power(self, proc, speed):
        """Returns average power"""
        proc = str(proc)
        speed = str(speed)
        key = proc + speed
        return self.average_power[key]

    def set_variation(self, proc, speed, variation_exec, variation_power):
        """Set variation for both exectime and power"""
        proc = str(proc)
        speed = str(speed)
        key = proc + speed
        self.variation_exec[key] = float(variation_exec)
        self.variation_power[key] = float(variation_power)

    def get_variation_exec(self, proc, speed):
        """Returns exec variation"""
        proc = str(proc)
        speed = str(speed)
        key = proc + speed
        return self.variation_exec[key]

    def get_variation_power(self, proc, speed):
        """Returns variation in power"""
        proc = str(proc)
        speed = str(speed)
        key = proc + speed
        return self.variation_power[key]

    def set_indegree(self, indegree):
        self.indegree = int(indegree)

    def get_indegree(self):
        return self.indegree

    def set_outdegree(self, outdegree):
        self.outdegree = int(outdegree)

    def get_outdegree(self):
        return self.outdegree

    def set_arcs(self, average, variation):
        """Sets the average and variations comm time for arcs"""
        self.arc_average = average
        self.arc_variation = variation

    def get_arcs_average(self):
        return self.arc_average

    def get_arcs_variation(self):
        return self.arc_variation

class DatHandler:
    """This class handles i.e. reads and writes into the dat file where info about the DAG is stored"""

    def set_path(self, path):
        self.path = path

    def write_dat(self, data):
        """This method writes the TaskGraphData object into the file specified in init"""
        data_file = open(self.path, 'w+')
        lines = []
        line1 = 'num_of_processors = ' + str(data.get_num_proc())
        lines.append(line1)
        line2 = '\nnum_of_tasks = ' + str(data.get_num_tasks())
        lines.append(line2)
        line3 = '\nindegree = ' + str(data.get_indegree())
        lines.append(line3)
        line4 = '\noutdegree = ' + str(data.get_outdegree())
        lines.append(line4)
        line5 = '\narcs ' + str(data.get_arcs_average()) + ' ' + str(data.get_arcs_variation())
        lines.append(line5)
        for proc in range(data.get_num_proc()):
            lines.append('\nnum_of_speeds_for_proc ' + str(proc) + ' = ' + str(data.num_speeds[proc]))
        for proc in range(data.get_num_proc()):
            lines.append('\n')
            lines.append('\nDetails of Processor ' + str(proc))
            lines.append('\nExecution  Times - Average - Variation')

            for speed in range(data.get_num_speeds(proc)):
                lines.append('\nexec ',proc,speed,'  ',data.get_average_exec(proc, speed),'  ',data.get_variation_exec(proc, speed))

            lines.append('\nPower Consumption - Average - Variation')

            for speed in range(data.get_num_speeds(proc)):
                lines.append('\npower ' + str(proc) + str(speed) + '  ' + str(data.get_average_power(proc, speed)) + '  ' + str(data.get_variation_power(proc, speed)))

        data_file.writelines(lines)
        data_file.close()

    def read_dat(self):
        """Reads the data file which helps to generate the tgffopt file"""
        self.data = TaskGraphData()
        data_file = open(self.path, 'r')
        lines = data_file.readlines()
        for line in lines:
            if (line.split()):
                if (line.split()[0] == 'num_of_processors'):
                    self.data.set_num_proc(int(line.split()[2]))
                if (line.split()[0] == 'num_of_tasks'):
                    self.data.set_num_tasks(int(line.split()[2]))
                if (line.split()[0] == 'indegree'):
                    self.data.set_indegree(int(line.split()[2]))
                if (line.split()[0] == 'outdegree'):
                    self.data.set_outdegree(int(line.split()[2]))
                if (line.split()[0] == 'arcs'):
                    self.data.set_arcs(float(line.split()[1]), float(line.split()[2]))
                if (line.split()[0] == 'exec'):
                    self.data.average_exec[line.split()[1]] = float(line.split()[2])
                    self.data.variation_exec[line.split()[1]] = float(line.split()[3])
                if (line.split()[0] == 'power'):
                    self.data.average_power[line.split()[1]] = float(line.split()[2])
                    self.data.variation_power[line.split()[1]] = float(line.split()[3])
        speeds = []
        for line_num in range(len(lines)):
            if (lines[line_num].split()):
                if (lines[line_num].split()[0] == 'num_of_speeds_for_proc'):
                    speeds.append(int(lines[line_num].split()[3]))
        self.data.set_num_speeds(speeds)

        return self.data

class TGFFGenerator:
    """This class generates the TGFF file it needs path to TGFF in the config file"""
    def __init__(self,data,tgff_filename):
        """Pass TaskGraphData and filename ONLY of the tgff file"""
        #print "I'm in TGFFGenerator __int__!"
        self.data = data
        self.tgff_path = "/home/kabir/Downloads/tgff-3.6/"
        self.tgff_path_example = self.tgff_path + 'examples/'
        self.tgff_filename = tgff_filename + '.tgffopt'
        self.write_file()

    def write_file(self):
        """Writes the tgffopt file to the examples directory of the tgff folder"""
        self.tgff_file = open(self.tgff_path_example+self.tgff_filename,'w+')
        lines = []
        lines.append('tg_cnt 1\n')
        lines.append('task_cnt ' + str(2*self.data.num_tasks) + ' 1\n')
        lines.append('task_degree ' + str(self.data.indegree) + ' ' + str(self.data.outdegree) +'\n')
        lines.append('task_type_cnt ' + str(self.data.num_tasks))
        lines.append('trans_type_cnt' + str(self.data.num_tasks*2))
        lines.append("\nperiod_laxity 1 \nperiod_mul 1,0.5,2 \ntg_write \neps_write \nvcg_write")
        #writing the details of processors into tgff file
        for proc in range(self.data.num_proc):
            lines.append('\ntable_label Proc' + str(proc) + 'Exec' + '\ntable_cnt 1' + '\ntype_attrib')
            for speed in range(self.data.num_speeds[proc]):
                if speed==0:
                    lines.append(' ')
                else:
                    lines.append(',')
                key = str(proc) + str(speed)
                average_exec = str(self.data.average_exec[key])
                variation_exec  = str(self.data.variation_exec[key])
                lines.append(' exec' + key + ' ' + average_exec + ' ' + variation_exec + ' 0.1')
            lines.append('\npe_write\n')
            lines.append('\ntable_label Proc' + str(proc) + 'Power')
            lines.append('\ntable_cnt 1')
            lines.append('\ntype_attrib ')
            for speed in range(self.data.num_speeds[proc]):
                if speed==0:
                    lines.append(' ')
                else:
                    lines.append(',')
                key = str(proc) + str(speed)
                average_power = str(self.data.average_power[key])
                variation_power  = str(self.data.variation_power[key])
                lines.append(' power' + key + ' ' + average_power + ' ' + variation_power + ' 0.1')
            lines.append('\npe_write\n')
        lines.append('\ntable_label arcs \ntable_cnt 1 \ntype_attrib time ' + str(self.data.arc_average) + ' ' + str(self.data.arc_variation) + '\ntrans_write')
        self.tgff_file.writelines(lines)
        self.tgff_file.close()

class TGFFcommand:
    """Runs the TGFF command"""
    def __init__(self,tgff_filename):
        self.tgff_filename = tgff_filename
        self.tgff_path = "/home/kabir/Downloads/tgff-3.6/"
        self.tgff_path_example = self.tgff_path + 'examples/'
        self.tgff_filename = tgff_filename + '.tgffopt'
        #retcode = subprocess.call([self.tgff_path+'tgff',self.tgff_path_example+tgff_filename])
        retcode = os.system(self.tgff_path+'tgff ' + self.tgff_path_example+tgff_filename)
        #print retcode

if __name__ == "__main__":
    print ("This is Start of main")
    data = TaskGraphData()
    """
    num_proc=input("Please input the number of processors: ")
    data.set_num_proc(num_proc)
    num_tasks=input("Please input the number of tasks in DAG: ")
    data.set_num_tasks(num_tasks)
    data.set_num_speeds()
    for i in range(int(num_proc)):
        for j in range(int(data.num_speeds[i])):
            print("Please enter for Average Execution Time for Processor ",i," at Speed ",j)
            exec_time = input("")
            print("Please enter for Variation in Execution Time for Processor ", i, " at Speed ", j)
            variation_exec = input("")
            print("Please enter for Average Power for Processor ",i," at Speed ",j)
            power = input("")
            print("Please enter for Variation in Power for Processor ", i, " at Speed ", j)
            variation_power = input("")
            data.set_average_exec(i, data.num_speeds[i], exec_time)
            data.set_average_power(i, data.num_speeds[i], power)
            data.set_variation(i, data.num_speeds[i], variation_exec, variation_power)
    in_degree=input("Please set the indegree of the DAG: ")
    data.set_indegree(in_degree)
    out_degree = input("Please set the outdegree of the DAG: ")
    data.set_outdegree(out_degree)
    average = input("PLease enter the average Arc/Edge value: ")
    variation = input("PLease enter the variation in Arc/Edge value: ")
    data.set_arcs(average, variation)
    """

    dat = DatHandler()
    dat.set_path('/home/kabir/PycharmProjects/DAGGeneration/example_case0.dat')
    #dat.write_dat(data)
    data = dat.read_dat()
    tg = TGFFGenerator(data, 'example_case0')
    tg.write_file()
    tc =TGFFcommand('example_case0')
    print("This is End of main")


