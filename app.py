from sys import argv
from server import TrackerService


TrackerService.start(len(argv) > 1)
