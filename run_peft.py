from .algorithms.peft import peft, gantt

comp_matrix_1 = peft.readCsvToNumpyMatrix('./graph_gen/task_exe_time.csv')
comm_matrix_1 = peft.readCsvToNumpyMatrix('./graph_gen/resource_BW.csv')
dag1 = peft.readDagMatrix('./graph_gen/task_connectivity.csv')

sched, _, _ = peft.schedule_dag(dag1, 
                                communication_matrix=comm_matrix_1, 
                                computation_matrix=comp_matrix_1)
gantt.showGanttChart(sched)
print(sched)