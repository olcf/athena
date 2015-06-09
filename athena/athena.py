'''
Athena - Fireworks-based workflow management tools for OLCF resources.

This module provides a loop for reovering offline jobs without invoking any
Fireworks CLI scripts in an attempt to supply missing API functionality.
'''

#from fireworks.queue.queue_launcher import launch_rocket_to_queue as qlaunch
from fireworks.queue.queue_launcher import rapidfire as rapid_qlaunch
from time import sleep
import sys

def process_offline(launchpad, launcher_args, daemon_mode=False):
    '''Continue to launch and recover runs offline while the launchpad has
    fireworks ready to run.
    '''

    # Launch first jobs.
    rapid_qlaunch(**launcher_args)

    def probe_offline_runs(all=False):
        '''Check the DB for at least one started offline run.'''
        if not all:
            return launchpad.offline_runs.find_one({'completed':False})
        else:
            return [i for i in launchpad.offline_runs.find({'completed':False})]

    try:
        while (probe_offline_runs() or launchpad.run_exists() or daemon_mode):
            current_job_launch_ids = sorted(
                    [i['launch_id'] for i in probe_offline_runs(all=True)]) 
            for launch_id in current_job_launch_ids: 
                launchpad.recover_offline(launch_id)
            # Slow down the hits to the DB.
            sleep(1)
            remaining_job_launch_ids = sorted(
                    [i['launch_id'] for i in probe_offline_runs(all=True)]) 
            if current_job_launch_ids != remaining_job_launch_ids:
                rapid_qlaunch(**launcher_args)
        else:
            print 'No jobs to run."'
    except KeyboardInterrupt:
        print 'Exiting on user command.'
        sys.exit(1) 
