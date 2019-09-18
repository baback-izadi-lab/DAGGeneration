
# Importing the matplotlb.pyplot
import matplotlib.pyplot as plt


def plot(schedule, max_time):

    # Declaring a figure "gnt"
    fig, gnt = plt.subplots()

    # Setting Y-axis limits
    gnt.set_ylim(0, 50)

    # Setting X-axis limits
    gnt.set_xlim(0, max_time)

    # Setting labels for x-axis and y-axis
    gnt.set_xlabel('seconds since start')
    gnt.set_ylabel('Processor')

    # Setting ticks on y-axis
    y_ticks = [35, 25, 15]
    gnt.set_yticks(y_ticks)
    # Labelling tickes of y-axis
    gnt.set_yticklabels(['1', '2', '3'])

    # Setting graph attribute
    gnt.grid(True)

    y_coords = [(30, 9), (20, 9), (10, 9)]
    proc_colors = ['tab:orange', 'tab:blue', 'tab:red']

    for i, proc in enumerate(schedule):
        sch = []
        for task in proc:
            sch.append((task[1], task[2] - task[1]))
            gnt.text((task[1]+task[2])/2,
                     y_ticks[i], "Task {}".format(task[0]),
                     horizontalalignment='center',
                     verticalalignment='center')
        gnt.broken_barh(
            sch, y_coords[i], facecolors=proc_colors[i], edgecolor='black')

    plt.savefig("gantt1.png")
