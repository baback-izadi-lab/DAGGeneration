from .algorithms.EDLS_mod import EDLS
from .utils.agent_schedule_runner import ScheduleRunner
from .utils.plot import plot
import json

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