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
                new_processes_ids.append(new_processes_id)


        return new_processes_ids


    """
    Function to monitor running process identity
    """
    def info(self, title):

        parent_process_id = os.getppid()
        process_id = os.getpid()
        module_name = __name__

        return {"parent_process_id":parent_process_id , "process_id":process_id, "process_title":title,"module_name":module_name}


    """
    Function for better visual in debugging
    """
    def pretty_debug(self, message=False, exp=False, email_sent=False, status_code=2):

            response = "\n message -> {}".format((str(message)))
            exp = exp

            message = message
            email_sent = email_sent
            status_code = status_code

            if (exp):
                response = response + "\n \033[4mException\033[0m -> \033[91m {} \033[0m".format(str(self.exp))

            print(response)


    """
    Function that starts the actual scrapping task
    This will be a subprocess call to run main.py
    """
    def do_job(self, name, the_shared_array, the_position, job_id):

        info = self.info('function do_job')
        self.job_processes_set.add(tuple({"job_id_{}".format(job_id): job_id, "process_id_{}".format(os.getpid()): os.getpid()}))
        self.pretty_debug(self.job_processes_set)
        self.pretty_debug('running {}'.format(info))
        self.pretty_debug("\nthe_processes_to_run: {} {}".format(the_shared_array, the_position))
        cmd = 'cd tests; python3 test_processes.py'
        subprocess.call(cmd,shell=True)
        array_pos = the_position - 1
        the_shared_array[array_pos] = 0
        time.sleep(2)


    def job_monitor(self):

        while True:

            time.sleep(10)
            self.pretty_debug("We are monitor all these processes {}".format(self.info("job monitor")))
            self.pretty_debug("Also we got {}".format(self.job_processes_set))



    def __init__(self):

        self.pretty_debug("Program main Thread {}".format(self.info("INITITAL THREAD OF EXECUTION")))

        if __name__ == '__main__':

            monitor_background_thread = Thread(name='process_analyser', target=self.job_monitor).start()
            self.job_processes_set = set()
            processes_to_run = self.cleanUp()
            self.job_processes_set = set()


            while( len(processes_to_run) < 5 ):

                if(len(processes_to_run) > 0):

                        self.pretty_debug("New process started")

                        shared_array = multiprocessing.Array("i", processes_to_run)

                        processes_to_run = [ shared_array[shared_value] for  shared_value in range(len(shared_array)) ]

                        for process_index, process_job_id in enumerate(processes_to_run):

                            process_name = "Process_{}".format(process_index)
                            p = Process(target=self.do_job, args=(process_name,shared_array, process_index, process_job_id))
                            processes_list.append(p)
                            p.start()

                        for each_process in processes_list:
                            each_process.join()


                        processes_to_run = [ shared_array[shared_value] for  shared_value in range(len(shared_array)) ]

                        try:
                            processes_to_run = list(set(processes_to_run)).remove(0)

                        except Exception as e:
                            self.pretty_debug(e)

                        if(processes_to_run == None):

                            processes_to_run = []

                        if(len(processes_to_run) == 0):

                            self.pretty_debug("Scheduling")
                            time.sleep(5)
                            processes_to_run = self.cleanUp()

                            if(len(processes_to_run) > 0):

                                shared_array = multiprocessing.Array("i", self.cleanUp())
                                processes_to_run = [ shared_array[shared_value] for  shared_value in range(len(shared_array)) ]

                else:
                    self.pretty_debug("No jobs found")
                    time.sleep(2)
                    processes_to_run = self.cleanUp()



viper = multi_viper()
