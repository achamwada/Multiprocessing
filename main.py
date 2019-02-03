"""
The problem
# Python Global Interpreter Lock restricts a process / program to only have 1 interpreter
# We need to find a way to monitor each process and take appropriate action based on
- A process that hangs on a task for at lease 30 mins
- 


Objectives
# Create processes this is because Python Global Interpreter Lock restrict a process to only have 1 interpreter

"""
import multiprocessing
from multiprocessing import Process
import os
import queue
import time
from model.DB_pooling import DBPooling
import subprocess
from threading import  Thread

database = DBPooling(1)

processes_list = []

class multi_viper():

    def cleanUp(self):

        new_processes_list = database.select("SELECT j.JobID FROM automation.jobs j where j.schedule < NOW() AND j.status = 'Not Started' LIMIT 4;")

        new_processes_ids = []

        if(len(new_processes_list) > 0 ):

            for new_process in new_processes_list:
                new_processes_id = new_process.get("JobID")
                #print(new_processes_id)
                new_processes_ids.append(new_processes_id)


        return new_processes_ids


    """
    Function to monitor running process identity
    """
    def info(self, title):
        print(title)
        print('module name:', __name__)
        print('parent process:', os.getppid())
        print('process id:', os.getpid())


    """
    Function that starts the actual scrapping task
    This will be a subprocess call to run main.py
    """
    def do_job(self, name, the_shared_array, the_position, job_id):
        self.info('function do_job')
        self.job_processes_set.add(tuple({"job_id_{}".format(job_id): job_id, "process_id_{}".format(os.getpid()): os.getpid()}))
        print(self.job_processes_set)
        #print(list(self.job_processes_set))
        print('running ', name)
        print("\nthe_processes_to_run: {} {}".format(the_shared_array, the_position))
        cmd = 'cd tests & C:\\Users\\achamwada\\AppData\\Local\\Programs\\Python\\Python37\\python.exe test_processes.py'
        subprocess.call(cmd,shell=True)
        array_pos = the_position - 1
        print(the_shared_array[array_pos])

        the_shared_array[array_pos] = 0
        # print(the_shared_array[array_pos])
        time.sleep(2)


    """
    Function to call new jobs if they are available from the jobs table
    """
    def scheduling(self):
        global shared_array, processes_to_run
        shared_array = multiprocessing.Array("i", self.cleanUp())

        processes_to_run = [shared_array[0], shared_array[1], shared_array[2], shared_array[3]]

        print("processes_to_run {}".format(processes_to_run))

        print("Before processes {} {} {} {}".format(shared_array[0], shared_array[1], shared_array[2], shared_array[3]))


    def job_monitor(self):
        while True:
            time.sleep(10)
            print("We are monitor all these processes")
            self.info("job monitor")
            print("Also we got {}".format(self.job_processes_set))



    def __init__(self):
        if __name__ == '__main__':
            self.info('main line')
            monitor_background_thread = Thread(name='process_analyser', target=self.job_monitor).start()
            self.job_processes_set = set()
            processes_to_run = self.cleanUp()

            #self.job_processes_set.add(tuple({"job_id_10":{"job_id":10, "process_id":2000}}))

            print(self.job_processes_set)


            while( len(processes_to_run) < 5 ):

                if(len(processes_to_run) > 0):
                        print("New process started")
                        #time.sleep(5)

                        shared_array = multiprocessing.Array("i", processes_to_run)

                        print("shared_array {}".format(shared_array))
                        print("shared_array len {}".format(len(shared_array)))

                        #processes_to_run = [shared_array[0], shared_array[1], shared_array[2], shared_array[3]]
                        processes_to_run = [ shared_array[shared_value] for  shared_value in range(len(shared_array)) ]
                        print("processes_to_run {}".format(processes_to_run))

                        print("processes_to_run {}".format(processes_to_run))

                        #print("Before processes {} {} {} {}".format(shared_array[0], shared_array[1], shared_array[2],shared_array[3]))

                        for process_index, process_job_id in enumerate(processes_to_run):

                            process_name = "Process_{}".format(process_index)
                            p = Process(target=self.do_job, args=(process_name,shared_array, process_index, process_job_id))
                            processes_list.append(p)
                            #processes_list.pop()
                            p.start()

                        for prcss in processes_list:
                            prcss.join()

                        # processes_to_run = [shared_array[0], shared_array[1], shared_array[2], shared_array[3]]
                        processes_to_run = [ shared_array[shared_value] for  shared_value in range(len(shared_array)) ]
                        #print("processes_to_run before {}".format(processes_to_run))
                        #print("After processes_to_run {}".format(processes_to_run))

                        try:
                            processes_to_run = list(set(processes_to_run)).remove(0)
                        except Exception as e:
                            print(e)

                        if(processes_to_run == None):
                            processes_to_run = []

                        if(len(processes_to_run) == 0):
                            print("Scheduling")
                            time.sleep(5)
                            processes_to_run = self.cleanUp()
                            #print(processes_to_run)

                            if(len(processes_to_run) > 0):
                                shared_array = multiprocessing.Array("i", self.cleanUp())

                                # processes_to_run = [shared_array[0], shared_array[1], shared_array[2], shared_array[3]]
                                processes_to_run = [ shared_array[shared_value] for  shared_value in range(len(shared_array)) ]

                else:
                    print("No jobs found")
                    time.sleep(2)
                    processes_to_run = self.cleanUp()


viper = multi_viper()