#!/usr/bin/env python
'''This script sets up a Fireworks workflow and launchpad using the API.

The objects defined here are constructed such that they can be used
interactively in an iterpereter shell or called via the command line to sanitize
a test database.

Most of the definitions here are typically replaced by static YAML configuration
files which can become cumbersome to manage in exotic use cases.
'''

__author__ = 'Matt Belhorn'
__version__ = '0.0.1'

#===============================================================================
# Setup constant environment information.

import os

PROJID = 'stf007'
MEMBERWORK = os.environ['MEMBERWORK']

src_dir = os.path.dirname(__file__)
run_dir = os.path.join(MEMBERWORK, PROJID)
log_dir = os.path.join(run_dir, 'log')
out_dir = os.path.join(run_dir, 'out')
bin_dir = '/ccs/home/belhorn/fireworks/athena/athena/bin'

# Safely make output directories.
for dir in (log_dir, out_dir):
    try:
        os.mkdir(dir)
    except OSError:
        pass


#===============================================================================
# Define the queue used by queue_launcher.  The machine running fireworks must
# be given a default queue to which jobs may be submitted. The individual args
# can be overridden by individual fireworks, so this just provides basic default
# values and is largely optional.

from fireworks.user_objects.queue_adapters.common_adapter import CommonAdapter

default_queue_adapter_args = {
        'q_type':'PBS',         # Queue adapter template to use.
        'q_name':'titan_queue', # FW name for the queue.
        'template_file':os.path.join(src_dir, 'templates', 'pbs.template'),
        'queue':'batch',        # PBS queue to use.
        'account':PROJID,       # OLCF allocation account.
        'logdir':log_dir,
        'nnodes':0,             # Overridden by tasks.
        'ppnode':0,             # Overridden by tasks.
        'walltime':'01:00:00',  # Overridden by tasks.
        'pre_rocket': 'module load python python_fireworks python_setuptools',
        'rocket_launch':'rlaunch singleshot --offline',
        'job_name':'unnamed_job',
        }
default_queue = CommonAdapter(**default_queue_adapter_args)



#===============================================================================
# Generate fake jobs and a sample workflow.

from fireworks import Firework, ScriptTask

class Jobs:
    def __init__(self, category, adapter):
        self.fireworks = {}
        self.adapter = dict(adapter)
        self.category = category
        self.spec = {'_queueadapter':self.adapter,
                     '_category':self.category,}

    def add(self, id, command, **kwargs):
        '''Add a task to the dictionary of fireworks.'''
        name = kwargs.pop('name', 'unspecified_task') 
        task = ScriptTask.from_str(command)
        firework = Firework(task, name=name, spec=self.spec) 
        self.fireworks[id] = firework

class Nodes:
    def __init__(self, category, default_adapter):
        self.compute = Jobs(category, default_adapter)
        self.service = Jobs(category, default_adapter)


titan = Nodes('titan', default_queue_adapter_args)
titan.compute.adapter.update({
        'walltime':'00:03:00',
        'nnodes':4,
        'ppnode':2,
        'queue':'killable',
        'job_name':'titan_compute_job',
        })
titan.service.adapter.update({
        'walltime':'00:05:00',
        'queue':'killable',
        'job_name':'titan_service_job',
        })
compute_binary = os.path.join(bin_dir, 'compute_node_task.sh')
for id in range(5):
    titan.compute.add(id,
            'aprun -n 1 -N 1 {} {}'.format(compute_binary, id),
            name='titan_compute_task_{}'.format(id))
    titan.service.add(id,
            'echo "Task service-{} running on `/bin/hostname`"'.format(id),
            name='titan_service_task_{}'.format(id))


rhea = Nodes('titan', default_queue_adapter_args)
rhea.compute.adapter.update({
        'walltime':'00:06:00',
        'nnodes':4,
        'queue':'batch@rhea-batch',
        'job_name':'rhea_compute_job',
        })
for id in range(2):
    rhea.compute.add(id,
            'mpirun -n 2 {} {}'.format(compute_binary, id),
            name='rhea_compute_task_{}'.format(id))



#================================================================================
# Generate a sample workflow using the 'fake' jobs.

wflow_links = {
        titan.service.fireworks[0]: [titan.compute.fireworks[0],
                                     titan.service.fireworks[1]],
        titan.compute.fireworks[0]: [titan.compute.fireworks[2],
                                     titan.compute.fireworks[3],
                                     titan.service.fireworks[2],
                                     titan.service.fireworks[3],
                                     rhea.compute.fireworks[0]],
        titan.service.fireworks[1]: [titan.compute.fireworks[1]],
        titan.compute.fireworks[1]: [titan.compute.fireworks[3],
                                     rhea.compute.fireworks[0]],
        rhea.compute.fireworks[0]:  [titan.service.fireworks[2]],
        titan.service.fireworks[2]: [titan.compute.fireworks[2]],
        titan.compute.fireworks[2]: [titan.compute.fireworks[4]],
        titan.service.fireworks[3]: [titan.compute.fireworks[3],
                                     rhea.compute.fireworks[1]],
        rhea.compute.fireworks[1]:  [],
        titan.compute.fireworks[3]: [titan.compute.fireworks[4]],
        titan.compute.fireworks[4]: [titan.service.fireworks[4]],
}


from fireworks import Workflow
workflow = Workflow(
        (titan.service.fireworks.values() +
                titan.compute.fireworks.values() +
                rhea.compute.fireworks.values()),
        wflow_links,
        name='sample_wf')



#==============================================================================
# Define the local worker. A worker definition can be supplied for the machine
# that is running qlauncher. This is entirely optional and only serves to
# restrict which jobs from the DB this instance of fireworks will run.

titan_worker_args = {
        'name':'titan',
        'category':'', # Only run a FireWork with a matching _category spec.
        'query':None,  # Only run a FireWork pulled from the DB with this query.
        'env':None,    # dict passed to firetasks _fw_env for resource_specific settings.
        }

from fireworks import FWorker
titan_worker = FWorker(**titan_worker_args)



#==============================================================================
# Define the launchpad (DB).

# FIXME: Using the queue system to run the daemon eliminates the utility of
# reading the DB password from stdin. It will HAVE to be in a file and until
# upstream can be modified, it will HAVE to be plaintext. Given these
# limitations, it will likely be best to use the cli  'lpad' etc from within the
# 'daemonized' PBS script.

#from getpass import getpass
db_args = {
        'host':'ds037087.mongolab.com',
        'port':37087,
        'name':'titan_fw_test',
        'username':'CHANGEME',
        'password':'CHANGEME',
        }
from secrets import db_secrets
db_args.update(db_secrets)

from fireworks import LaunchPad
launchpad = LaunchPad(**db_args)

# Define the launch args to be passed to queue_launcher.
launcher_args = {
        'launchpad':launchpad,
        'fworker':titan_worker,
        'qadapter':default_queue, # Individual jobs override this.
        'launch_dir':out_dir,
        'reserve':True,           # Necessary for offline job submission.
        'strm_lvl':'CRITICAL',
}

# Bring the 'daemon' loop into scope.
from athena import process_offline



#==============================================================================
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
            description='Demonstrates Fireworks workflows on OLCF resources.')
    parser.add_argument('--reset', action='store_true',
            help='Reset the database.')
    parser.add_argument('-p', '--push-wf', dest='push_wf', action='store_true',
            help='Push demo workflow to DB.')
    parser.add_argument('-b', '--block', action='store_true',
            help='Create a new output block.')
    parser.add_argument('-u', '--update-only', action='store_true',
            help='Update the DB, but do not process jobs.')
    parser.add_argument('-d', '--daemon-mode', action='store_true',
            help='Update the DB, but do not process jobs.')
    args = parser.parse_args()

    if args.reset:
        launchpad.reset('', require_password=False)

    if args.push_wf:
        launchpad.add_wf(workflow)

    if args.block:
        from fireworks.utilities.fw_utilities import create_datestamp_dir
        create_datestamp_dir(out_dir, launchpad.m_logger)

    if not args.update_only:
        process_offline(launchpad, launcher_args, args.daemon_mode)

