from .data_representation import TGFFParser
#from EDLS import EDLS
from ..algorithms.EDLS_mod import EDLS
import json
#from schedule_runner import ScheduleRunner
from .agent_schedule_runner import ScheduleRunner
from itertools import product
import pandas as pd
from collections import defaultdict
import random
import tqdm


# Data for simulation
parser = TGFFParser()
# For small DAGS

total_processors = 5
#total_tasks = 200

if total_processors == 3:
    total_speeds = [1, 2, 3]
    all_base_powers = [[5], [5, 6], [5, 6, 7]]
elif total_processors == 5:
    total_speeds = [1, 2, 3, 3, 3]
    p = 1.0
    all_base_powers = [[85*p], [130*p, 85*p], [205*p, 130*p, 85*p],
                       [205*p, 130*p, 105*p], [205*p, 130*p, 115*p]]


def run(beta, all_speeds, base_path, sim_data, algorithm_name,
        dls_algo=False, base_power_min=True, agent_system=True):
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
        json.dump(agent_schedule, open(
            base_path+f'agent_schedule_{algorithm_name}.json', 'w'))

        # Agent system runs here
        runner = ScheduleRunner(schedule=agent_schedule,
                                speed_setting=processor_speeds,
                                dag_data=edls.dag.data,
                                base_powers=base_powers, beta=beta,
                                agent_system=agent_system)

        runner.start()

        # Results of the run are collected here
        table_data['Processor Combination'].append(processor_speeds)
        table_data['Task Energy'].append(runner.task_energy)
        table_data['Total Execution Time'].append(runner.max_time)
        table_data['Idle Energy'].append(sum(runner.idle_energy))
        table_data['Total Energy'].append(
            runner.task_energy+sum(runner.idle_energy))

    table = pd.DataFrame.from_dict(table_data)
    return table


def run_all(total_tasks, algorithm_name, dls=False):
    # TGFF Parser from data representation
    # Parses TGFF file and converts to json
    base_path = f'./results/DAG{total_tasks}/'
    sim_data = 'task_data.json'

    data = parser.parse(base_path+f'DAG_{total_tasks}_5.tgff',
                        total_processors, total_speeds)

    # parser.write_json(base_path+sim_data)

    # Generates all possible speeds
    all_speeds = []
    for speeds in total_speeds:
        all_speeds.append([i for i in range(speeds)] + [None])

    # beta_values = [(0.6, 1.0)]
    beta_values = [1.0]
    #algorithm_name = 'TBH'
    #dls = False

    writer = pd.ExcelWriter(
        base_path + f'{algorithm_name}_{total_tasks}.xlsx', engine='xlsxwriter')
    for beta in beta_values:
        all_processors_speeds = list(product(*all_speeds))
        labels = ['Task Energy', 'Total Execution Time',
                  'Idle Energy', 'Total Energy']
        total_table_data = defaultdict(list)
        total_table_data['Processor Combination'] = [
            comb for comb in all_processors_speeds]

        for label in labels:
            total_table_data[label] = [
                0 for i in range(len(all_processors_speeds))]

        total_table = pd.DataFrame.from_dict(total_table_data)
        beta_name = beta

        num_of_runs = 1
        beta_list = []
        for exp_run in range(num_of_runs):
            if isinstance(beta, tuple):
                beta = [random.uniform(beta[0], beta[1])
                        for task_num in range(len(parser.tasks))]
            beta_list.append(beta)

        for beta in beta_list:
            table = run(beta, all_speeds, base_path, sim_data, algorithm_name,
                        dls_algo=dls, base_power_min=True, agent_system=True)
            for label in labels:
                total_table[label] += table[label]

        for label in labels:
            total_table[label] /= num_of_runs

        total_table.to_excel(
            writer, sheet_name='beta {} agents'.format(beta_name))

        total_table = pd.DataFrame.from_dict(total_table_data)
        for beta in beta_list:
            table = run(beta, all_speeds, base_path, sim_data, algorithm_name,
                        dls_algo=dls, base_power_min=False, agent_system=False)
            for label in labels:
                total_table[label] += table[label]

        for label in labels:
            total_table[label] /= num_of_runs
        table.to_excel(
            writer, sheet_name='beta {} wo agents'.format(beta_name))
    writer.save()


if __name__ == '__main__':
    total_tasks = [25, 50, 100, 200, 400]
    #total_tasks = [25]
    for i in tqdm.trange(len(total_tasks), desc='DAG'):
        tasks = total_tasks[i]
        run_all(tasks, 'EDLS_mod', dls=False)
        # print(f'EDLS for DAG {tasks} complete')

    # for tasks in total_tasks:
    #    run_all(tasks, 'DLS', dls=True)
    #    print(f'DLS for DAG {tasks} complete')
