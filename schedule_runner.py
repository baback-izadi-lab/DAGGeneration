import json


class Task:
    def __init__(self, task_num, processor, speed, exec_time, exec_power, comm_data):
        self.task_num = task_num
        self.processor = processor
        self.speed = speed
        self.clock_time = 0
        self.exec_time = exec_time
        self.exec_power = exec_power
        self.comm_data = comm_data

    def set_graphs(self, parents, children, processor_times):
        self.parents = parents
        self.children = children
        self.processor_times = processor_times

    def message(self, task, clock_time):
        if clock_time > self.clock_time:
            self.clock_time = clock_time
        if task in self.parents:
            self.parents.remove(task)
        if len(self.parents) == 0:
            self.start_time = self.clock_time
            self.clock_time += self.exec_time
            self.end_time = self.start_time + self.exec_time
            print('Task {}: Proc: {} Exec Time = {:.3f}-{:.3f}'.format(self.task_num,
                                                                       self.processor,
                                                                       self.start_time,
                                                                       self.end_time))
            # task_stats = {'processor': self.processor, 'task': self.task_num,
            #              'start': self.start_time, 'end': self.end_time}
            energy = self.exec_time * self.exec_power
            task_stats = (self.task_num, round(self.start_time, 3),
                          round(self.end_time, 3), round(energy, 3))
            self.processor_times[self.processor].append(task_stats)
            for child in self.children:
                if child.processor == self.processor:

                    child.message(self.task_num, self.clock_time)
                else:
                    child.message(self.task_num, self.clock_time +
                                  self.comm_data[(self.task_num, child.task_num)])


if __name__ == "__main__":
    all_data = json.load(open('test_simple.json'))
    schedule = json.load(open('agent_schedule.json'))
    all_tasks = {}
    comm_data = {}
    for arc, data in all_data['arcs'].items():
        comm_data[(data[0], data[1])] = data[2]
    for task_detail in schedule:
        task = int(task_detail)
        processor = schedule[task_detail]['processor']
        speed = schedule[task_detail]['speed']
        exec_time = all_data['proc_exec'][str(processor)][speed][task]
        exec_power = all_data['proc_power'][str(processor)][speed][task]
        t = Task(task, processor, speed, exec_time, exec_power, comm_data)
        all_tasks[task] = t

    processor_times = [[] for i in range(len(all_data['proc_exec']))]
    for task_num, task in all_tasks.items():
        parents = set(schedule[str(task_num)]['parents'])
        children = set()
        for child_num in schedule[str(task_num)]['children']:
            children.add(all_tasks[child_num])
        task.set_graphs(parents, children, processor_times)

    all_tasks[0].message(None, 0)
    print(processor_times)
    task_energy = sum([task[3] for proc in processor_times for task in proc])
    print(task_energy)
    max_time = max([task[2] for proc in processor_times for task in proc])
    print(max_time)

    import plot

    plot.plot(processor_times, max_time)
