import os
import json

import lda.setup
import bills.setup
import matching.setup
import firms.setup
import analytics.setup
import tariffs.setup

CONFIG_FILE = "./code/config.json"
def main():
    # print "*************** Installing Requirements *****************\n"
    # import subprocess
    # subprocess.call("pip install -r requirements.txt", shell=True)

    path = os.path.join(os.getcwd(), "./", CONFIG_FILE)

    with open(path, 'r') as f:
        config = json.loads(f.read())
        config = config.get("setup")
        if config.get("matching"):
          print "*************** Building Data ***************************\n"
          matching.setup.main()
        if config.get("bills"):
          print "*************** Building Bills **************************\n"
          bills.setup.main()
        if config.get("lda"):
          print "*************** Building LDA ****************************\n"
          lda.setup.main()
        if config.get("firms"):
          print "*************** Building Firms **************************\n"
          firms.setup.main()
        if config.get("tariffs"):
          print "*************** Building miscellaneous **********************\n"
          tariffs.setup.main()
        if config.get("analytics"):
          print "*************** Building Analytics **********************\n"
          analytics.setup.main()

if __name__ == '__main__':
    main()