from pathlib import Path
import pandas as pd
import json

class PEFTGraphParser:
    def __init__(self):
        pass

    def parse(self, task_conn_path, task_exe_path, proc_powers):
        task_conn_path = Path(task_conn_path)
        task_exe_path = Path(task_exe_path)
        self.task_conn_df = pd.read_csv(task_conn_path)
        self.task_exe_df = pd.read_csv(task_exe_path)

        self.total_tasks = len(self.task_exe_df)
        self.total_procs = len(proc_powers)

        self.final_data = {}
        self.final_data['arcs'] = self.read_conn()
        self.final_data['proc_exec'] = self.read_exe()
        self.final_data['proc_power'] = self.read_powers(proc_powers)

    def read_conn(self):
        arcs = {}
        arc_num = 0
        for i, row in self.task_conn_df.iterrows():
            for t in range(self.total_tasks):
                if row[f'T_{t}'] != 0.0:
                    arcs[arc_num] = (i, t, row[f'T_{t}'])
                    arc_num += 1
        return arcs

    def read_exe(self):
        proc_exec = {}
        for p in range(self.total_procs):
            proc_exec[p] = [list(self.task_exe_df[f'P_{p}'].to_numpy())]
        return proc_exec

    def read_powers(self, powers):
        proc_powers = {}
        for i, power in enumerate(powers):
            p = [power for x in range(self.total_tasks)]
            proc_powers[i] = [p]
        return proc_powers

    def write_json(self, output_file):
        """ Write to json file"""
        json.dump(self.final_data, open(output_file, 'w'))

if __name__ == '__main__':
    parser = PEFTGraphParser()
    parser.parse('task_connectivity.csv', 'task_exe_time.csv', [85, 105, 115])
    parser.write_json('task.json')