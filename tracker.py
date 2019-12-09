from sys import argv
from server import TrackerService


TrackerService.start('random' in argv[1:])
