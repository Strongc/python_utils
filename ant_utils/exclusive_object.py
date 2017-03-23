
# -*- coding: utf-8 -*-

import sys, os, shutil, tempfile
from subprocess import Popen, PIPE
import time

class ExclusiveObject(object):
    def __init__(self, path, wait=0):
        self.pid_file = '%s/%s.pid' % (tempfile.gettempdir(), path.replace('/', '_'))
        i = 0
        while True:
            if os.path.isfile(self.pid_file):
                cmd = "ps ax | awk '{ print $1; }'"
                process = Popen(cmd, shell=True, stdout=PIPE)
                stdout, stderr = process.communicate()
                pid = open(self.pid_file, 'rb').read().strip()
                if pid in stdout.split():
                    if i >= wait:
                        sys.exit(-1)
                    else:
                        time.sleep(1)
                        i += 1
                        continue
            break
        open(self.pid_file, 'w').write(str(os.getpid()))

    def __del__(self):
        if open(self.pid_file, 'rb').read() == str(os.getpid()):
            os.remove(self.pid_file)

