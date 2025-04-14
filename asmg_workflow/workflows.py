#-----------------------------------------------------------------------------
# Name:        workflows
# Purpose:    
# Authors:      
# Created:     11/23/2020
# License:     MIT License
#-----------------------------------------------------------------------------
"""workflows == a module that allows a series of tasks to be organized as a workflow. Workflows are have a start task,
a series of tasks with or without dependencies, and a stop task. 

"""
#-----------------------------------------------------------------------------
# Standard Imports
import sys
import os
import time
import datetime
import networkx

#-----------------------------------------------------------------------------
# Third Party Imports
sys.path.append(os.path.join(os.path.dirname(__file__),".."))
from asmg_workflow.tasks import *
from asmg_workflow.utils import concurrently

import matplotlib.pyplot as plt
import networkx as nx
from pyvis.network import Network

#-----------------------------------------------------------------------------
# Module Constants

#-----------------------------------------------------------------------------
# Module Functions

#-----------------------------------------------------------------------------
# Module Classes


class WorkflowSerializer():
    """Saves the workflow to disk or to a database"""
    pass

class Workflow(Task):
    """Workflow == a class that organizes Tasks based on their dependencies. First create a set of tasks
    and then add them to the workflow with workflow.add_task(Task). workflow.execute() then runs all of the tasks. """
    def __init__(self,**options ):
        defaults = {"name":auto_name("New","workflow",None,"").replace(".",""),
                   "auto_log":True,
                   "log_serializer": YamlSerializer(),"log":True}
        self.task_options = {}
        broadcast_option_keys = ["log","log_serializer","auto_log"]
        self.broadcast_options = {}
        for key,value in defaults.items():
            self.task_options[key]=value
        for key,value in options.items():
            self.task_options[key] = value
        super(Workflow, self).__init__(**self.task_options)
        for key,value in self.task_options.items():
            if key in broadcast_option_keys:
                self.broadcast_options[key]=value

        self.tasks = []
        self.schedule = []
        start_task = StartTask(name = f"Start_{self.task_options['name']}",**self.broadcast_options)
        stop_task = StopTask(name = f"Start_{self.task_options['name']}",**self.broadcast_options)
        self.tasks.append(start_task)
        self.tasks.append(stop_task)
        self.graph = None

    def add_start_task(self,task):
        self.tasks[0] = task

    def add_stop_task(self,task):
        self.tasks[-1] = task

    def add_task(self,task):
        """Adds a task to the self.tasks attribute"""
        self.tasks.insert(1,task)

    def check_time(self):
        "Returns the current time for the workflow process and sets self.time"
        now = datetime.datetime.now()
        self.time = now
        return now

    def reset_workflow(self):
        """Resets all of the tasks to task.started = False and task.completed = False"""
        if self.task_options["log"]:
            self.log.add_entry("Reset the workflow")
        for task in self.tasks:
            task.started = False
            task.completed = False
            task.output = None

    def arrange_tasks(self):
        """Makes the schedule for the workflow"""
        if len(self.tasks) == 2:
            number_slots= 2
        else:
            max_depth = max(list(map(lambda x:x.depth,self.tasks)))
            number_slots = max_depth+3
        self.schedule = [[] for i in range(number_slots)]
        for task_index,task in enumerate(self.tasks):
            if task_index == 0:
                self.schedule[0].append(task)
            elif task_index == len(self.tasks)-1:
                self.schedule[-1].append(task)
            else:
                self.schedule[task.depth+1].append(task)


    def execute(self,exit_condition = "concurrently",verbose = False):
        """Runs the workflow"""
        self.started = True
        self.arrange_tasks()
        if exit_condition == "serial_start":
            for slot in self.schedule:
                for task in slot:
                    try:
                        task.check_dependencies()
                        if False in task.met_dependencies:
                            break
                        else:
                            task.execute()
                            if self.task_options["log"]:
                                self.log.add_entry(f"Executed task {task.name}")
                        if self.task_options["log"]:
                            self.log = self.log + task.log

                    except :
                        pass

        if exit_condition == "concurrently":
            for slot_index,slot in enumerate(self.schedule):
                if verbose:
                    print(f"on slot {slot_index}")
                n_loops = 0
                while slot:
                    n_loops += 1
                    ready_to_run =[]
                    ready_to_run_index_list = []
                    for task_index, task in enumerate(slot):
                        task.check_dependencies()
                        if False in task.met_dependencies:
                            break
                        else:
                            ready_to_run.append(task)
                            ready_to_run_index_list.append(task_index)
                    concurrently(*list(map(lambda x: x.execute,ready_to_run)))
                    if self.task_options["log"]:
                        self.log.add_entry(f"Starting slot {slot_index}, tasks {list(map(lambda x: x.name,ready_to_run))} have started")
                    if verbose:
                        print(f"Currently on loop {n_loops}")
                    n_rr_loops = 0
                    while ready_to_run:
                        if verbose:
                            print(f"Currently on ready to run loop {n_rr_loops}")
                        n_rr_loops += 1
                        try:
                            current_task = ready_to_run[0]
                            curent_slot_index = ready_to_run_index_list[0]
                            if current_task.completed:
                                if self.task_options["log"]:
                                    self.log.add_entry(f"The task {slot[curent_slot_index].name} has completed")
                                ready_to_run.pop()
                                slot.pop(curent_slot_index)

                        except Exception as e:
                            self.on_error(e)

        if exit_condition == "on_start":
            for slot_index,slot in enumerate(self.schedule):
                if verbose:
                    print(f"on slot {slot_index}")
                n_loops = 0
                while slot:
                    if verbose:
                        print(f"Currently on loop {n_loops}")
                    n_loops += 1
                    try:
                        current_task = slot[0]
                        if current_task.started:
                            slot.pop()
                        else:
                            current_task.check_dependencies()
                            if False in current_task.met_dependencies:
                                break
                            else:
                                current_task.execute()
                                if self.task_options["log"]:
                                    self.log.add_entry(f"Started task {current_task.name}")
                                if verbose:
                                    print(f"Starting Task {current_task.name}")
                                slot.pop()
                    except Exception as e:
                        self.on_error(e)

        if exit_condition == "wait_till_completed":
            for slot_index, slot in enumerate(self.schedule):
                if verbose:
                    print(f"on slot {slot_index}")
                n_loops = 0
                while slot:
                    if verbose:
                        print(f"Currently on loop {n_loops}")
                    n_loops += 1
                    try:
                        current_task = slot[0]
                        if current_task.completed:
                            slot.pop()
                            if self.task_options["log"]:
                                self.log.add_entry(f"The task {current_task.name} has completed")
                        else:
                            if current_task.started:
                                break
                            else:
                                current_task.check_dependencies()
                                if False in current_task.met_dependencies:
                                    break
                                else:
                                    current_task.execute()
                                    if self.task_options["log"]:
                                        self.log.add_entry(f"Started task {current_task.name}")
                                    if verbose:
                                        print(f"Starting Task {current_task.name}")
                                    slot.pop()
                    except Exception as e:
                        self.on_error(e)

        if exit_condition == "until_slot_completed":
            for slot_index, slot in enumerate(self.schedule):
                if verbose:
                    print(f"on slot {slot_index}")
                n_loops = 0
                task_index = 0
                while slot:
                    if verbose:
                        print(f"Currently on loop {n_loops}")
                    n_loops += 1
                    if verbose:
                        print(f"Ths slot == now {slot}")
                    try:
                        if verbose:
                            print(f"Task index == {task_index}")
                        current_task = slot[task_index]
                        if verbose:
                            print(f"Current task == {current_task}")
                            print(f"task.completed it {current_task.completed}, and task.started == {current_task.started}")
                        if current_task.completed:
                            if self.task_options["log"]:
                                self.log.add_entry(f"{current_task.name} completed")
                            popped = slot.pop(task_index)
                            if verbose:
                                print(f"The task {popped} was popped from slot")
                                print(f"Ths slot == now {slot}")
                            #self.log = self.log + current_task.log
                        else:

                            if current_task.started:
                                task_index = (task_index + 1) % len(slot)
                                break
                            else:
                                current_task.check_dependencies()
                                if False in current_task.met_dependencies:
                                    task_index = (task_index + 1) % len(slot)
                                    break
                                else:
                                    current_task.execute()
                                    if self.task_options["log"]:
                                        self.log.add_entry(f"Started task {current_task.name}")
                                    if verbose:
                                        print(f"Starting Task {current_task.name}")
                    except Exception as e:
                        self.on_error(e)

        self.completed = True



    def restart(self):
        """Restarts a partially executed workflow, does not reset tasks, for that use workflow.reset()"""
        self.execute()

    def on_error(self,exception):
        raise exception

    def show(self,display = "slot",**options):
        defaults={"type_":"nx","notebook":False,"verbose":False}
        show_options = {}
        for key,value in defaults.items():
            show_options[key] = value
        for key,value in options.items():
            show_options[key] = value
        self.arrange_tasks()
        self.graph = nx.MultiDiGraph()
        if display == 'slot':
            for slot_index,slot in enumerate(self.schedule):
                if show_options["verbose"]:
                    print(f"The slot index == {slot_index}")
                if slot_index == 0:
                    self.graph.add_node("StartTask")
                elif slot_index == len(self.schedule)-1:
                    self.graph.add_node("StopTask")
                else:
                    for task_index,task in enumerate(slot):
                        if show_options["verbose"]:
                            print(f"The task index == {task_index} and the task == {task}")
                        self.graph.add_node(f"Slot {slot_index} Task {task_index}")
                        if "task" in list(map(lambda x:x["type"],task.dependencies)):
                            if show_options["verbose"]:
                                print("task == in the list of dependencies")
                            for dependency in task.dependencies:
                                if dependency["type"] == "task":
                                    if show_options["verbose"]:
                                        print("task == the dependency type ")
                                    slot_back = 1
                                    n_loops = 0
                                    while slot_index - slot_back > 0 and n_loops<len(self.schedule):
                                        try:
                                            index = self.schedule[slot_index-slot_back].index(dependency["input"]["task"])
                                            self.graph.add_edge(f"Slot {slot_index-1} Task {index}",
                                                                f"Slot {slot_index} Task {task_index}")
                                        except:
                                            slot_back += 1
                                        n_loops += 1
                    if slot_index == len(self.schedule)-2:
                        for task_index,task in enumerate(slot):
                            if show_options["verbose"]:
                                print(f"The task index == {task_index} and the task == {task}")
                            self.graph.add_node(f"Slot {slot_index} Task {task_index}")
                            self.graph.add_edge(f"Slot {slot_index} Task {task_index}","StopTask")
                    elif slot_index == 1:
                        for task_index, task in enumerate(slot):
                            if show_options["verbose"]:
                                print(f"The task index == {task_index} and the task == {task}")
                            self.graph.add_node(f"Slot {slot_index} Task {task_index}")
                            self.graph.add_edge("StartTask", f"Slot {slot_index} Task {task_index}")
            if len(self.schedule)==2:
                self.graph.add_edge("StartTask","StopTask")

        elif display == 'name':
            for slot_index,slot in enumerate(self.schedule):
                if show_options["verbose"]:
                    print(f"The slot index == {slot_index}")
                if slot_index == 0:
                    self.graph.add_node(self.tasks[0].name)
                elif slot_index == len(self.schedule)-1:
                    self.graph.add_node(self.tasks[-1].name)
                else:
                    for task_index,task in enumerate(slot):
                        if show_options["verbose"]:
                            print(f"The task index == {task_index} and the task == {task.name}")
                        self.graph.add_node(f"{task.name}")
                        if "task" in list(map(lambda x:x["type"],task.dependencies)):
                            if show_options["verbose"]:
                                print("task == in the list of dependencies")
                            for dependency in task.dependencies:
                                if dependency["type"] == "task":
                                    if show_options["verbose"]:
                                        print("task == the dependency type ")
                                    slot_back = 1
                                    n_loops = 0
                                    while slot_index - slot_back > 0 and n_loops<len(self.schedule):
                                        try:
                                            index = self.schedule[slot_index-slot_back].index(dependency["input"]["task"])
                                            dependent_task = self.schedule[slot_index-slot_back][index]
                                            self.graph.add_edge(f"{dependent_task.name}",
                                                                f"{task.name}")
                                        except:
                                            slot_back += 1
                                        n_loops += 1
                    if slot_index == len(self.schedule) - 2:
                        for task_index, task in enumerate(slot):
                            if show_options["verbose"]:
                                 print(f"The task index == {task_index} and the task == {task}")
                            self.graph.add_node(f"{task.name}")
                            self.graph.add_edge(f"{task.name}", f"{self.tasks[-1].name}")

                    if slot_index == 1:
                        for task_index, task in enumerate(slot):
                            if show_options["verbose"]:
                                print(f"The slot index == {slot_index}, The task index == {task_index} and the task == {task}")
                            self.graph.add_node(f"{task.name}")
                            self.graph.add_edge(f"{self.tasks[0].name}", f"{task.name}")
                            if show_options["verbose"]:
                                print(f"Adding the edge {self.tasks[0].name},{task.name} ")

            if len(self.schedule)==2:
                self.graph.add_edge(self.tasks[0].name,self.tasks[-1].name)

        if show_options['type_'] == 'nx':
            self.display_layout = networkx.spring_layout(self.graph)
            nx.draw(self.graph, with_labels=True, font_weight='bold')
            if not show_options["notebook"]:
                plt.show()

        elif show_options['type_'] == 'pyvis':
            net = Network(notebook=show_options['notebook'])
            net.from_nx(self.graph)
            net.show_buttons(filter_=['physics'])
            net.show("example.html")

#-----------------------------------------------------------------------------
# Module Scripts


def test_Workflow():
    workflow = Workflow(log=False)
    print(dir(workflow))
    workflow.show()

def test_adding_tasks():
    def f(x):
        print("X squared!")
        return x**2
    def f2(x):
        print("X cubed!")
        return x**3
    print("Creating a new function task ...")
    input_variable = 3.2
    new_task_0 = FunctionTask(function = f, args=[input_variable],log=False)
    new_task_1 = FunctionTask(function = f, args=[input_variable],log=False)
    output_variable =0
    dependency = TypeDependency(input  = {"variables":{"x":input_variable},"types":{"x":(int,float)}})
    task_dependency = Dependency(type='task', on_fail='error', checker='task_checker',
                            input  = {"task":new_task_1})
    new_task_1.add_dependency(dependency)
    new_task_1.check_dependencies()
    #new_task_1.execute()
    new_task_2 = FunctionTask(function=f2, args=[output_variable],log=False)
    new_task_2.add_dependency(task_dependency)
    workflow = Workflow(log=False)
    workflow.add_task(new_task_0)
    workflow.add_task(new_task_1)
    workflow.add_task(new_task_2)
    workflow.execute()
    print(workflow.tasks)

    print(workflow.schedule)
    print(workflow.log)
    workflow.reset_workflow()
    workflow_2 = Workflow()
    workflow_2.add_task(workflow)
    print(workflow_2.tasks)
    workflow_2.execute()
    workflow.show()

#-----------------------------------------------------------------------------
# Module Runner


if __name__ == '__main__':
    #test_adding_tasks()
    test_Workflow()