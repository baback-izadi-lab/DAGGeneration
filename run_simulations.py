from data_representation import TGFFParser
from EDLS import EDLS
import json
#from schedule_runner import ScheduleRunner
from agent_schedule_runner import ScheduleRunner
from itertools import product
import pandas as pd
from collections import defaultdict


parser = TGFFParser()
total_processors = 3
total_speeds = [1, 2, 3]
all_base_powers = [[5], [5, 15], [5, 15, 25]]

base_path = './results/EDLS/DAG-10/'
# data = parser.parse(base_path+'example_case1.tgff',
#                    total_processors, total_speeds)
#sim_data = 'task_data.json'
sim_data = 'test_simple.json'
# parser.write_json(base_path+sim_data)


all_speeds = []
for speeds in total_speeds:
    all_speeds.append([i for i in range(speeds)] + [None])


def run(beta, dls_algo=False, base_power_min=True):
    table_data = defaultdict(list)
    all_processors_speeds = product(*all_speeds)
    for processor_speeds in all_processors_speeds:
        if base_power_min:
            base_powers = [min(power) for power in all_base_powers]
        else:
            base_powers = [power[processor_speeds[i]] if processor_speeds[i] is not None else 0
                           for i, power in enumerate(all_base_powers)]

        if tuple([None for i in range(total_processors)]) == processor_speeds:
            continue
        # Note if you want to run DLS algorithm uncoment following command
        edls = EDLS(base_path+sim_data)
        schedule = edls.run(processor_speeds, dls_algo=dls_algo)
        agent_schedule = edls.get_agent_schedule()
        json.dump(agent_schedule, open(base_path+'agent_schedule.json', 'w'))

        runner = ScheduleRunner(schedule=base_path+'agent_schedule.json',
                                dag_data=base_path+sim_data,
                                base_powers=base_powers, beta=beta)

        runner.start()
        table_data['Processor Combination'].append(processor_speeds)
        table_data['Task Energy'].append(runner.task_energy)
        table_data['Total Execution Time'].append(runner.max_time)
        table_data['Idle Energy'].append(runner.idle_energy)
        table_data['Total Energy'].append(
            runner.task_energy+runner.idle_energy)

    table = pd.DataFrame.from_dict(table_data)
    if dls_algo:
        algo = 'DLS'
    else:
        algo = 'EDLS'

    if base_power_min:
        power_setting = 'min'
    else:
        power_setting = 'sch'
    table.to_csv(base_path+'{}_beta_{}_power_{}.csv'.format(algo,
                                                            beta, power_setting), float_format='%.3f')


beta_values = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, (0.85, 1.0)]

for beta in beta_values:
    run(beta, dls_algo=False,  base_power_min=True)

run(1.0, dls_algo=True, base_power_min=True)
