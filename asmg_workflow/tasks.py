#-----------------------------------------------------------------------------
# Name:        tasks
# Purpose:    
# Authors:      
# Created:     11/23/2020
# License:     MIT License
#-----------------------------------------------------------------------------
"""tasks is a module that holds the predefined tasks for the asmg_workflow software. A Task is a class that represents
an abstract task to be done. It must have the attributes task.started and task.completed. In addition,
the method task.execute() is designed to be overloaded to define exactly what happens when the task is run. The
class Dependency defines a condition that should be checked before a task executes.

"""
#-----------------------------------------------------------------------------
# Standard Imports
import sys
import os
import datetime
import subprocess
import traceback
import re
import time
import uuid
#-----------------------------------------------------------------------------
# Third Party Imports
sys.path.append(os.path.join(os.path.dirname(__file__),".."))
from asmg_workflow.logs import *
import pyvisa as visa
import pandas as pd
#-----------------------------------------------------------------------------
# Module Constants

#-----------------------------------------------------------------------------
# Module Functions
def print_time(duration = 100):
    "A function that prints the time every 1 second for duration to test tasks"
    duration = datetime.timedelta(seconds=duration)
    start_time = datetime.datetime.now()
    current_time = datetime.datetime.now()
    while current_time-start_time < duration:
        print(current_time)
        time.sleep(1)
        current_time = datetime.datetime.now()

def always_true(dependency):
    "Test checker that always returns True"
    return True

def always_false(dependency):
    "Test checker that always returns False"
    return False

def timer(dependency):
    """Timer checker that checks if a time in input = {'absolute_time':Datetime} has elapsed or
    if a relative time specified by input = {'relative_time':TimeDelta,'start_time':DateTime} has elapsed"""
    now = datetime.datetime.now()
    if "absolute_time" in dependency["input"].keys():
        if now>dependency["input"]["absolute_time"]:
            return True
        else:
            return False
    if "relative_time" in dependency["input"].keys():
        delta = now-dependency["input"]["start_time"]
        print(f'delta is {delta}, relative_time is {dependency["input"]["relative_time"]}')
        if delta>dependency["input"]["relative_time"]:
            return True
        else:
            return False

def time_of_day_checker(dependency):
    """Time of day checker determines if it is currently between start time and stop time"""
    pass

def task_checker(dependency):
    """Determines if a the task in input = {'task':Task} has been completed """
    if dependency["input"]["task"].completed:
        return True
    else:
        return False

def task_value_type_checker(dependency):
    """Determines if the task is done and the output is off the type input ={'task':Task,'ouptut_name':name,'output_type':int}"""
    completed = task_checker(dependency)
    type = True
    if not isinstance(dependency["input"]['task'].output,dependency["input"]['output_type']):
        type = False
    return completed and type

def type_checker(dependency):
    output=True
    for key,value in dependency["input"]["variables"].items():
        if not isinstance(value,dependency["input"]["types"][key]):
            output = False
    return output

def file_exists_checker(dependency):
    """Checks to see if a file exits, requires the dependency to have input  = {'file':fileName}"""
    if os.path.isfile(dependency["input"]["file"]):
        return True
    else:
        return False

def visa_resource(dependency):
    rm = visa.ResourceManager()
    resource = rm.open_resource(dependency['input']['resource_name'])
    idn = resource.query("IDN?")
    if re.match(dependency["input"]["idn"],idn,re.IGNORECASE):
        return True
    else:
        return False

def ip_resource(dependency):
    pass

def ue_resource(dependency):
    pass
#-----------------------------------------------------------------------------
# Module Classes

class DependencyError(Exception):
    pass

class TaskError(Exception):
    pass

class Dependency(dict):
    """A dependency is simply a dictionary with the keys: type, input,on_fail, checker, duration,
    number_repeats, repeat_until. The value of on_fail determines the behavior of the task when it fails the
     dependency check, the possible values are pass, repeat_n_times, repeat_until, repeat_for"""

    def __init__(self,type = None, input = None, on_fail='error',checker = 'always_true',duration = None,
                 number_repeats = None,repeat_time = None):
        self['type']= type
        self['input'] = input
        self['on_fail'] = on_fail
        self['checker'] = checker
        self['duration'] = duration
        self['number_repeats'] = number_repeats
        self['repeat_time'] = repeat_time

class TimeOfDayDependency(Dependency):
    """Built to make sure it is a specific time of day on a 24 hour clock"""
    def __init__(self, hour_start = 12, minute_start = 0, second_start = 0 ,
                  hour_stop = 24, minute_stop = 0, second_stop = 0  ):
        pass

class TaskDependency(Dependency):
    def __init__(self, type = 'task', input = None, on_fail='error',checker = 'task_checker',duration = None,
                 number_repeats = None,repeat_time = None):
        super(TaskDependency,self).__init__(type = type, input = input, on_fail=on_fail,checker =checker)
        """Represents a task dependency or only do the next task if this one is done, the input should be of the form
        input = {'task':Task}"""

class TaskOutputDependency(Dependency):
    def __init__(self, type = 'task', input = None, on_fail='error',checker = 'task_checker',duration = None,
                 number_repeats = None,repeat_time = None):
        super(TaskOutputDependency,self).__init__(type = type, input = input, on_fail=on_fail,checker =checker)
        """Represents a task dependency or only do the next task if this one is done and if the 
        output is of the right type, the input should be of the form
        input = {'task':Task,'output_name':'variable_name','output_type':int}"""

class TypeDependency(Dependency):
    def __init__(self, type = 'type', input = None, on_fail='error',checker = 'type_checker',duration = None,
                 number_repeats = None,repeat_time = None):
        super(TypeDependency,self).__init__(type = type, input = input, on_fail=on_fail,checker =checker)
        """Represents a type dependency (unit test), the input should be of the form
        input = {'variables':{'x':input_variable},'types':{'x':int}} or
        input = {'variables':{'x':input_variable},'types':{'x':(int,float)}}"""

class VisaDependency(Dependency):
    def __init__(self, type = 'visa', input = None, on_fail='error',checker = 'visa_resource',duration = None,
                 number_repeats = None,repeat_time = None):
        """Dependency that checks if a visa resource responds with the right idn. The input should be of the form
        input = {'resource_name':VISAResource,'idn':ReturnedIDN}"""
        super(VisaDependency,self).__init__(TypeDependency,
                                            self).__init__(type = type, input = input, on_fail=on_fail,checker =checker)

class Task():
    """An abstract representation of a task to be completed"""
    def __init__(self,path = None,**options):
        defaults = {"name":auto_name("New","Task",None,"").replace(".",""),
                    "auto_log":True,
                    "log_serializer":YamlSerializer(),"log":True}
        self.task_options = {}
        for key,value in defaults.items():
            self.task_options[key]=value
        for key,value in options.items():
            self.task_options[key] = value
        if path:
            self.path = path
        else:
            self.path  = auto_name("New","Task",None,"task")
        self.name = self.task_options['name']
        self.dependencies = []
        self.met_dependencies=[]
        self.dependency_repeats = []
        self.dependency_times = []
        self.dependency_durations = []
        self.id = uuid.uuid4()
        if self.task_options["log"]:
            self.log = Log(file_path = auto_name(self.name,"log",os.getcwd(),self.task_options["log_serializer"].extension),
                        auto_save =self.task_options["auto_log"],serializer  = self.task_options["log_serializer"])
            self.log.add_entry(f"Task {self.name} was created.")
            self.log.add_entry(f"Task id is {self.id}")
        self.start_time = datetime.datetime.now()
        self.timer = datetime.datetime.now()
        self.get_task_duration()
        self.completed = False
        self.retries=0
        self.max_tries = 1
        self.depth = 0
        self.started = False

    def count_task_depth(self,dependency):
        """Counts the depth of task dependencies"""
        depth = 0
        try:
            new_task = dependency["input"]["task"]
            depth = 1 + new_task.depth
        except:
            pass
        return depth

    def add_dependency(self,dependency):
        """Adds a dependency to the list Task.dependencies"""
        self.get_task_duration()
        self.dependencies.append(dependency)
        self.met_dependencies.append(False)
        self.dependency_repeats.append(0)
        self.dependency_times.append(self.timer)
        self.dependency_durations.append(self.duration)
        depth = self.count_task_depth(dependency)
        if depth>self.depth:
            self.depth = depth
        new_entry = dict(dependency)
        new_entry["event"] = f"A {dependency['type']} dependency was added."
        if self.task_options["log"]:
            self.log.add_entry(new_entry)

    def remove_dependency(self,dependency):
        """Removes a dependency in the list Task.dependencies"""
        remove_index = self.dependencies.index(dependency)
        self.dependencies.pop(remove_index)
        self.met_dependencies.pop(remove_index)
        self.dependency_repeats.pop(remove_index)
        self.dependency_times.pop(remove_index)
        self.dependency_durations.pop(remove_index)

    def execute(self):
        """Causes the task to run"""
        try:
            self.started = True
            self.completed  = True
        except Exception as e:
            self.retries += 1
            self.on_error(e)

    def get_task_duration(self):
        """Returns the current run time for the task"""
        self.timer = datetime.datetime.now()
        self.duration = self.timer - self.start_time
        return self.duration

    def throw_dependency_error(self,dependency):
        """Raises the dependency error"""
        msg = f"The task {self.name} failed at the dependency {dependency['type']}"
        if self.task_options["log"]:
            self.log.add_entry(msg)
        raise DependencyError(msg)

    def on_error(self,exception):
        """Function that executes on an error in the execution of the task"""
        raise exception

    def check_dependencies(self):
        """Iterates through the list self.dependencies and checks if they are satisfied"""
        for dependency_index,dependency in enumerate(self.dependencies):
            # is there a cleaner way of doing this?
            dependency_met=globals()[dependency["checker"]](dependency)
            if dependency_met:
                msg = f"The task {self.name} met the dependency {dependency['type']}"
                if self.task_options["log"]:
                    self.log.add_entry(msg)
                self.met_dependencies[dependency_index] = True
            else:
                action = dependency["on_fail"]
                # switch statement to handle a dependency check failure
                if action in ["pass","retry"]:
                    msg = f"The task {self.name} did not meet the dependency {dependency['type']} and is passing"
                    if self.task_options["log"]:
                        self.log.add_entry(msg)
                    else:
                        print(msg)

                elif action in ["error"]:
                    self.throw_dependency_error(dependency)

                elif action in ["repeat_until"]:
                    # the time handler
                    print(f'The dependency is {dependency}')
                    self.current_dependency_repeat_time = dict(dependency)["repeat_time"]
                    self.get_task_duration()
                    self.dependency_times[dependency_index] = self.timer
                    print(f'self.timer is {self.timer} and repeat time is {self.current_dependency_repeat_time }')
                    if self.timer<self.current_dependency_repeat_time:
                        msg = f"""The task {self.name} did not meet the dependency {dependency['type']} at {self.timer} 
                               and is repeating until {dependency['repeat_time']}"""
                        if self.task_options["log"]:
                            self.log.add_entry(msg)
                        else:
                            print(msg)
                    else:
                        self.throw_dependency_error(dependency)

                elif action in ["repeat_n_times"]:
                    self.dependency_repeats[dependency_index]+=1
                    print(f"self.dependency repeats is {self.dependency_repeats}")
                    if dependency['number_repeats']>self.dependency_repeats[dependency_index]:
                        msg = f"""The task {self.name} did not meet the dependency 
                                    {dependency['type']} for the {self.dependency_repeats[dependency_index]} time
                                    and is repeating {dependency['number_repeats']} times"""
                        if self.task_options["log"]:
                            self.log.add_entry(msg)
                        else:
                            print(msg)
                    else:
                        self.throw_dependency_error(dependency)

                elif action in ["repeat_for"]:
                    duration = self.get_task_duration()
                    self.dependency_durations[dependency_index]=duration
                    if dependency["duration"]>duration:
                        msg = f"""The task {self.name} did not meet the dependency 
                                    {dependency['type']} and has been running {duration} which is less than the allotted 
                                    duration of {dependency["duration"]}"""
                        if self.task_options["log"]:
                            self.log.add_entry(msg)
                        else:
                            print(msg)
                    else:
                        self.throw_dependency_error(dependency)


class FunctionTask(Task):
    def __init__(self,path = None, function = None, args = [] ,kwargs = {},**options):
        super(FunctionTask,self).__init__(path,**options)
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.output = None

    def execute(self, args = [] ,kwargs = {}):
        if args:
            self.args = args
        if kwargs:
            self.kwargs = kwargs
        try:
            if self.task_options["log"]:
                self.log.add_entry(f"Execution of function {self.function} has begun")
            self.started = True
            self.output = self.function(*self.args,**self.kwargs)
            if self.task_options["log"]:
                self.log.add_entry(f"Execution of function {self.function} has completed")
            self.completed = True
            return self.output
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback)
            self.on_error(e)


class DependentFunctionTask(Task):
    def __init__(self, path=None, function=None, task_dictionary = {}, **options):
        """Assumes the input for the function is the output of the task dependencies"""
        #todo: clean up how the function(task.output) is done
        super(DependentFunctionTask, self).__init__(path, **options)
        self.function = function
        self.output = None
        self.task_outputs = []

    def execute(self):
        try:
            self.task_outputs = []
            for dependency in self.dependencies:
                #print(f"The dependencies are {self.dependencies}")
                if "task" in dependency.values():
                    task = dependency['input']['task']
                    #print(f"The task dependency has this output: {task.output}")
                    self.task_outputs.append(task.output)
            if self.task_options["log"]:
                self.log.add_entry(f"Execution of function {self.function} has begun")
            self.started = True
            #print(f"The output of the previous function is {self.task_outputs}")
            self.output = eval(f"self.function(*{self.task_outputs})")
            if self.task_options["log"]:
                self.log.add_entry(f"Execution of function {self.function} has completed")
            self.completed = True
            return self.output
        
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback)
            self.on_error(e)

class MultipleDependentFunctionTask(Task):
    def __init__(self, path=None, function=None, task_dictionary = {}, args = [] ,kwargs = {},**options):
        """Assumes the input for the function is the output of the task dependencies"""
        #todo: clean up how the function(task.output) is done
        super(MultipleDependentFunctionTask, self).__init__(path, **options)
        self.function = function
        self.output = []
        self.task_outputs = {}
        self.args = args
        self.kwargs = kwargs

    def execute(self):
        try:
            for dependency in self.dependencies:
                if "task" in dependency.values():
                    task = dependency['input']['task']
                    self.kwargs[dependency['input']['output_name']]=task.output
            if self.task_options["log"]:
                self.log.add_entry(f"Execution of function {self.function} has begun")
            self.started = True
            self.output = eval(f"self.function(*{self.args},**{self.kwargs})")
            if self.task_options["log"]:
                self.log.add_entry(f"Execution of function {self.function} has completed")
            self.completed = True
            return self.output
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback)
            self.on_error(e)

class ShellTask(Task):
    """Creates a task that writes the string command+arguments to the command line when executed"""
    def __init__(self,path  = None,command = 'start "" cmd /c "echo Hello world!&echo(&pause"',arguments  = "",**options):
        super(ShellTask, self).__init__(path,**options)
        self.command  = command
        self.arguments = arguments

    def execute(self):
        try:
            self.started = True
            subprocess.run(self.command+self.arguments,shell=True)
            self.completed = True
        except Exception as e:
            self.on_error(e)

class StartTask(Task):
    """Container for starting tasks in a workflow"""
    def __init__(self,path = None,name = auto_name("Start","Task",None,"task"), **options):
        super(StartTask, self).__init__(path =path,name = name,**options)


class StopTask(Task):
    """Container for the last task in a work flow"""
    def __init__(self,path = None,name  = auto_name("Stop","Task",None,"task"),**options):
        super(StopTask, self).__init__(path =path,name = name, **options)

class SimpleVisaTask(Task):
    """Sends a single command to a visa resource"""
    def __init__(self,path = None,resource_name = None, command = "IDN?",mode = "query",**options):
        super(SimpleVisaTask,self).__init__(path = path, **options)
        if path:
            self.name = path
        else:
            self.name  = auto_name("Visa","Task",None,"task")
        self.mode = mode
        self.command = command
        self.output = None
        self.resource_name = resource_name

    def execute(self):
        try:
            rm = visa.ResourceManager()
            resource = rm.open_resource(self.resource_name)
            self.started = True
            self.output = resource.__getattribute__(self,self.mode)(self.command)
            self.completed = True
        except Exception as e:
            self.on_error(e)

class FunctionalExperimentTask(Task):
    """Runs an experiment based on a function. The function should return data as a single dictionary"""

    def __init__(self, function, run_list=None, **options):
        defaults = {"name": auto_name("New", "Experiment", None, "task"),
                    "finished_path": auto_name("finished", "points", os.getcwd(), "csv"),
                    "output_path": auto_name("experiment", "output", os.getcwd(), "csv"),
                    "dependent_run_list": False}

        self.task_options = {}
        for key, value in defaults.items():
            self.task_options[key] = value
        for key, value in options.items():
            self.task_options[key] = value
        self.function = function
        if isinstance(run_list, list):
            self.run_list = run_list
        elif isinstance(run_list, str):
            input_df = pd.read_csv(run_list)
            self.run_list = input_df.to_dict('records')
        else:
            self.run_list = run_list
        self.output_path = self.task_options["output_path"]
        self.output = os.path.join(os.getcwd(), self.output_path)
        self.finished_path = self.task_options["finished_path"]
        super(FunctionalExperimentTask, self).__init__(**self.task_options)

    def append_csv_row(self, filepath, point, header=False, index=False):
        """appends a row to the finshed csv"""
        df = pd.DataFrame([point])
        df.to_csv(filepath, mode='a', header=header, index=index)

    def execute(self):
        try:
            if self.task_options["dependent_run_list"]:
                for dependency in self.dependencies:
                    if "task" in dependency.values():
                        task = dependency['input']['task']
                        self.run_list = eval(r" task.output")
            self.finished = []
            self.output = []
            if self.task_options["log"]:
                self.log.add_entry(f"Execution of the experiment has begun")
            self.started = True
            n = 0
            while self.run_list:
                try:
                    output_point = {}
                    run_point = self.run_list.pop(0)
                    msg = "Running"
                    log_entry = {}
                    log_entry["event"] = msg
                    log_entry["loop_number"] = n
                    log_entry.update(run_point)
                    if self.task_options["log"]:
                        self.log.add_entry(log_entry)
                    result = self.function(**run_point)
                    self.finished.append(run_point)
                    output_point.update(run_point)
                    output_point.update(result)
                    output_point["timestamp"] = datetime.datetime.now()
                    if n == 0:
                        self.append_csv_row(self.finished_path, run_point, header=True)
                        self.append_csv_row(self.output_path, output_point, header=True)
                    else:
                        self.append_csv_row(self.finished_path, run_point, header=False)
                        self.append_csv_row(self.output_path, output_point, header=False)
                    n += 1
                except Exception as e:
                    if self.task_options["log"]:
                        self.log.add_entry({"error": e, "loop_number": n})
                    self.on_point_error(e)
                    print(e)
                    pass
            self.completed = True
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback)
            self.on_error(e)

    def on_point_error(self, exception):
        pass

#-----------------------------------------------------------------------------
# Module Scripts
def test_MultipleDependentFunction():
    def f(x):
        print("Function f is executing")
        return x**2
    def f2(x):
        print("Function f2 is executing ")
        return x+2
    def f3(x,y):
        return x+y
    task1 = FunctionTask(function=f,args=[2],name="square")
    task2 = FunctionTask(function=f2, args=[3], name="add_2")
    task3 = MultipleDependentFunctionTask(function=f3,name="sum_of_both")
    dependency1 = TaskDependency(input={'task':task1,'output_name':"x"})
    dependency2 = TaskDependency(input={'task':task2,'output_name':"y"})
    task3.add_dependency(dependency1)
    task3.add_dependency(dependency2)
    task1.execute()
    print(f"task1 named {task1.name} whose output is {task1.output} has executed")
    task2.execute()
    print(f"task2 named {task2.name} whose output is {task2.output} has executed")
    task3.execute()
    print(f"task3 named {task3.name} whose output is {task3.output} has executed")

def test_Dependency(number_repeats=31,repeat_time = datetime.datetime.now()+datetime.timedelta(seconds=32),
                    absolute_time = datetime.datetime.now()+datetime.timedelta(seconds=30)):
    import time
    print("Creating a new Dependency Class")
    dependency = Dependency(type='Test',on_fail='repeat_n_times',checker = 'always_false',number_repeats=number_repeats)
    print("The contents of the dependency are {0}".format(dependency))
    new_task = Task()
    print(new_task)
    new_task.add_dependency(dependency)

    dependency_2 = Dependency(type='Timer', on_fail='repeat_until', checker='timer',
                              repeat_time=repeat_time,
                              input = {"absolute_time":absolute_time})
    new_task.add_dependency(dependency_2)
    for i in range(101):
        time.sleep(1)
        print(f"iteration : {i}")
        print(new_task.log)
        new_task.check_dependencies()
        print(new_task.met_dependencies)
        print(new_task.log)
        print(new_task.dependency_repeats)
        print(new_task.dependency_times)

def test_two_FunctionTasks():
    """Tests dependent FunctionTasks"""
    def f(x):
        print("Function f is executing")
        return x**2
    def f2(x):
        print("Function f2 is executing ")
        return x+2
    print("Creating a new function task ...")
    new_task = FunctionTask(function = f, args=[3])
    print(f"New task directory is {dir(new_task)}")
    out = new_task.execute()
    print(f"The task returned {out}")
    print(f"The task log is {new_task.log}")
    print("Creating a new function task ...")
    input_variable = 3
    new_task = FunctionTask(function = f, args=[input_variable])
    print("Creating a new type dependency and adding it to task 1")
    dependency = TypeDependency(input={"variables": {"x": input_variable}, "types": {"x": (int, float)}})
    new_task.add_dependency(dependency)
    print("Creating a second function task")
    new_task_2 = FunctionTask(function = f2, kwargs={"x":new_task.output})
    print("Creating a task dependency and adding it to task 2")
    task_dependency = Dependency(type='task', on_fail='error', checker='task_checker',
                            input  = {"task":new_task})
    new_task_2.add_dependency(task_dependency)
    print("Executing task 1 after checking the dependencies")
    new_task.check_dependencies()
    new_task.execute()
    #new_task_2.args = [new_task.output]
    print(f"The task 2 arguments are {new_task_2.kwargs}")
    new_task_2.check_dependencies()
    new_task_2.execute()
    print(f"The output of task 2 is {new_task_2.output}")

def test_DependentFunctionTask():
    """Tests dependent FunctionTasks"""
    def f(x):
        print("Function f is executing")
        return x**2
    def f2(x):
        print("Function f2 is executing ")
        return x+2
    print("Creating a new function task ...")
    new_task = FunctionTask(function = f, args=[3])
    print(f"New task directory is {dir(new_task)}")
    out = new_task.execute()
    print(f"The task returned {out}")
    print(f"The task log is {new_task.log}")
    print("Creating a new function task ...")
    input_variable = 3
    new_task = FunctionTask(function = f, args=[input_variable])
    print("Creating a new type dependency and adding it to task 1")
    dependency = TypeDependency(input={"variables": {"x": input_variable}, "types": {"x": (int, float)}})
    new_task.add_dependency(dependency)
    print("Creating a second function task")
    new_task_2 = DependentFunctionTask(function = f2)
    print("Creating a task dependency and adding it to task 2")
    task_dependency = Dependency(type='task', on_fail='error', checker='task_checker',
                            input  = {"task":new_task})
    new_task_2.add_dependency(task_dependency)
    print("Executing task 1 after checking the dependencies")
    new_task.check_dependencies()
    new_task.execute()
    #new_task_2.args = [new_task.output]
    print(f"The task 2 arguments are {new_task_2.task_outputs}")
    new_task_2.check_dependencies()
    new_task_2.execute()
    print(f"The output of task 2 is {new_task_2.output}")

def test_FunctionTask():
    """Simple test of the FunctionTask class"""
    def f(x):
        return x**2
    def f2(x):
        return x**3
    print("Creating a new function task ...")
    input_variable = 3
    new_task = FunctionTask(function = f, args=[input_variable],log=False)
    print(f"New task directory is {dir(new_task)}")

def test_type_checker():
    """Tests the type checker """
    def f(x):
        return x**2
    def f2(x):
        return x**3
    print("Creating a new function task ...")
    input_variable = 3
    new_task = FunctionTask(function = f, args=[input_variable])
    print(f"New task directory is {dir(new_task)}")
    dependency = Dependency(type='type', on_fail='error', checker='type_checker',
                            input  = {"variables":{"x":input_variable},"types":{"x":(int,float)}})
    new_task.add_dependency(dependency)
    new_task.check_dependencies()
    out = new_task.execute()
    print(f"The task returned {out}")
    print(f"The task log is {new_task.log}")

def test_task_checker():
    """Tests a task dependency and it's checker by making two simple tasks and executing them serially
    and printing diagnostic information."""
    def f(x):
        return x**2
    def f2(x):
        return x**3
    print("Creating a new function task ...")
    input_variable = 3.2
    new_task_1 = FunctionTask(function = f, args=[input_variable])
    output_variable =0
    print(f"New task directory is {dir(new_task_1)}")
    dependency = TypeDependency(input  = {"variables":{"x":input_variable},"types":{"x":(int,float)}})
    task_dependency = Dependency(type='task', on_fail='error', checker='task_checker',
                            input  = {"task":new_task_1})
    new_task_1.add_dependency(dependency)
    new_task_1.check_dependencies()
    new_task_1.execute()
    new_task_2 = FunctionTask(function=f2, args=[new_task_1.output])
    new_task_2.add_dependency(task_dependency)
    print(f"The 1st task returned {output_variable}")
    print(f"The  1st task log is {new_task_1.log}")
    new_task_2.check_dependencies()
    print(f"the value of input is {new_task_2.args}")
    out_2 = new_task_2.execute()
    print(f"The 2nd task returned {out_2}")
    print(f"The  2nd task log is {new_task_2.log}")
    print("*"*80)
    print(f"The first task depth is {new_task_1.depth}")
    print(f"The second task depth is {new_task_2.depth}")

def test_chained_tasks(n_tasks = 100 ):
    "Tests a series of tasks that depend on the previous one"
    tasks = []
    first_task =FunctionTask(function = lambda x:x , args=[1])
    old_task = first_task
    old_task.execute()
    for i in range(n_tasks):
        # create a task
        new_function = lambda x: i*x+i
        new_task = FunctionTask(function = new_function, args=[old_task.output])
        # add  a dependency
        task_dependency = Dependency(type='task', on_fail='error', checker='task_checker',
                                     input={"task": old_task})
        new_task.add_dependency(task_dependency)
        tasks.append(new_task)
        print(f"The new task depth is {new_task.depth}")
        # make the new_task the old_task
        old_task = new_task
        old_task.execute()

    for task_index,task in enumerate(tasks):
        print("*"*80)
        print(f"Checking Dependencies for task number {task_index}")
        task.check_dependencies()
        print(task.log[-1])
        print("Executing the function ...")
        #task.execute()
        print(f"The function returns {task.output}")


def test_Task():
    """Creates a new task and prints the directory"""
    new_task = Task()
    print(dir(new_task))

def test_ShellTask(command='start "" cmd /c "echo Hello world!&echo(&pause"'):
    """Test's the ShellTask Class by passing it command and executing it """
    st = ShellTask(command=command)
    st.check_dependencies()
    st.execute()
    print(f"The ShellTask.completed is {st.completed}")

def test_schedule():
    tasks =[]
    tasks.append(StartTask())
    for i in range(10):
        tasks.append(Task())
    tasks.append(StopTask)
    schedule = [[],[],[]]
    for task_index,task in enumerate(tasks):
        if task_index == 0:
            schedule[0].append(tasks[0])
        elif task_index == len(tasks)-1:
            schedule[-1].append(tasks[-1])
        else:
            schedule[1].append(task)
    for slot in schedule:
        while slot:
            task_done = slot.pop()
            print(f"Task {task_done} was popped from slot")
#-----------------------------------------------------------------------------
# Module Runner

if __name__ == '__main__':
    test_FunctionTask()
    #test_Dependency()
    #test_Task()
    #test_type_checker()
    #test_task_checker()
    #test_ShellTask()
    #test_chained_tasks()
    #test_schedule()
    #test_two_FunctionTasks()
    #test_DependentFunctionTask()
    #test_MultipleDependentFunction()