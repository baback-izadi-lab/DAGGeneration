from data_representation import TGFFParser
from EDLS import EDLS
import json
#from schedule_runner import ScheduleRunner
from agent_schedule_runner import ScheduleRunner
from itertools import product
import pandas as pd
from collections import defaultdict
import random


# Data for simulation
parser = TGFFParser()
# For small DAGS
#total_processors = 3
#total_speeds = [1, 2, 3]
#all_base_powers = [[5], [5, 15], [5, 15, 25]]
#all_base_powers = [[5], [5, 6], [5, 6, 7]]

# For Large DAGS

total_processors = 5
total_speeds = [1, 2, 3, 3, 3]
all_base_powers = [[5], [5, 6], [5, 6, 7], [5, 6, 7], [5, 6, 7]]


# TGFF Parser from data representation
# Parses TGFF file and converts to json
base_path = './results/TEDLS-NB/DAG-400/'
data = parser.parse(base_path+'example_case7.tgff',
                    total_processors, total_speeds)

sim_data = 'task_data.json'
parser.write_json(base_path+sim_data)

# Generates all possible speeds
all_speeds = []
for speeds in total_speeds:
    all_speeds.append([i for i in range(speeds)] + [None])


def run(beta, dls_algo=False, base_power_min=True, agent_system=True):
    """
    This functions runs the agent system based on certain parameters

    params:
        beta: float from 0.0 to 1.0
        dls_algo: bool. DLS used if True, else use EDLS
        base_power_min: bool. Decides how to calculate idle energy
                        If True: Assume processor switches to slowest speed
                        during idle periods, which implies lowest energy
                        If False: Assume processor stays idle at specified speed

    """

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

        # Agent system runs here
        runner = ScheduleRunner(schedule=base_path+'agent_schedule.json',
                                speed_setting=processor_speeds,
                                dag_data=base_path+sim_data,
                                base_powers=base_powers, beta=beta,
                                agent_system=agent_system)

        runner.start()

        # Results of the run are collected here
        table_data['Processor Combination'].append(processor_speeds)
        table_data['Task Energy'].append(runner.task_energy)
        table_data['Total Execution Time'].append(runner.max_time)
        table_data['Idle Energy'].append(runner.idle_energy)
        table_data['Total Energy'].append(
            runner.task_energy+runner.idle_energy)

    table = pd.DataFrame.from_dict(table_data)
    return table


#

beta_values = [1.0, (0.85, 1.0)]

writer = pd.ExcelWriter(base_path + 'EDLS_min.xlsx', engine='xlsxwriter')
for beta in beta_values:
    beta_name = beta
    if isinstance(beta, tuple):
        beta = [random.uniform(beta[0], beta[1])
                for task_num in range(len(parser.tasks))]
        # beta = [random.uniform(beta[0], beta[1])
        #        for task_num in range(10)]

    table = run(beta, dls_algo=False,  base_power_min=True, agent_system=True)
    table.to_excel(writer, sheet_name='beta {} agents'.format(beta_name))
    table = run(beta, dls_algo=False,
                base_power_min=False, agent_system=False)
    table.to_excel(writer, sheet_name='beta {} wo agents'.format(beta_name))
writer.save()
