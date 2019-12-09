from os import system
from sys import argv
from threading import Thread
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
    Thread(target=start_tracker, args=('random' in argv[1:],)).start()
    Thread(target=start_client).start()
