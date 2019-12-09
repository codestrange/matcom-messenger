from os import system
from sys import argv
from multiprocessing import Process
from server import TrackerService


def start_tracker(random: bool):
    TrackerService.start(random)

def start_client():
    system("""
    export FLASK_APP=client/run.py &&
    export FLASK_ENV=production    &&
    export FLASK_DEBUG=0           &&
    flask run
    """)

if __name__ == "__main__":
    Process(target=start_tracker, args=('random' in argv[1:],)).start()
    Process(target=start_client).start()
