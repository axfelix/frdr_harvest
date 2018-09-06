import os
import sys


class Lock:
    """ Try to prevent multiple instances of the program running at once """

    def __init__(self):
        self.lockfile = None
        if os.name == 'posix':
            fcntl = __import__('fcntl')

            try:
                self.lockfile = open('lockfile', 'w')
                self.lockfile.write(str(os.getpid()) + "\n")
                self.lockfile.flush()
            except:
                sys.stderr.write(
                    "ERROR: was harvester runnning under a different user previously? (could not write to lockfile)\n")
                raise SystemExit

            try:
                os.chmod('lockfile', 0o664)
            except:
                pass

            try:
                fcntl.flock(self.lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (OSError, IOError):
                sys.stderr.write("ERROR: is harvester already running? (could not lock lockfile)\n")
                raise SystemExit

    def unlock(self):
        if os.name == 'posix':
            fcntl = __import__('fcntl')
            fcntl.flock(self.lockfile, fcntl.LOCK_UN)
            self.lockfile.close()
            os.unlink('lockfile')
