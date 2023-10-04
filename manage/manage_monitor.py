#!/usr/bin/env python3

#
# Ulf Andrae, SMHI, 2023
#

import hashlib
import os
import shutil
import sys
import yaml
import json
import subprocess
import tarfile
import glob


from argparse import ArgumentParser
from datetime import datetime, timedelta

class Exp():
    def __init__(self, name, config):

        self.name = name
        self.cfg = config
        self.toc_file = f"{name}_toc.json"
        self.toc = { "vfld" : {}, "done" : {}}
        if os.path.isfile(self.toc_file):
           self.toc = json.loads(self.toc_file)
        self.toc["vfld"] = self._ecfs_scan(f"{self.cfg['ecfs_path']}/{name}/vfld")

    def _ecfs_scan(self,path):

      cmd = subprocess.Popen(["els", path], stdout=subprocess.PIPE)
      cmd_out, cmd_err = cmd.communicate()
  
      # Decode and filter output
      res = [line.decode("utf-8").replace(f"vfld{self.name}","") for line in cmd_out.splitlines()]
      res = [x.replace(f".tar","") for x in res]
      res = [datetime.strptime(x, "%Y%m") for x in res]

      return res

    def prepare_data(self,month=None):

        if month is None:
            for date in self.toc["vfld"]:
                if date not in self.toc["done"]:
                    month = date
                    break
        m = month.strftime("%Y%m")
        ym = month.strftime("%Y/%m")
        data_path = f"{self.cfg['data_path']}/{ym}".replace("user",os.environ["USER"])
        os.makedirs(data_path, exist_ok=True)
        os.chdir(data_path)
        for t,n in {"vfld" : self.name ,"vobs": ""}.items():
           ecfs_path = f"{self.cfg['ecfs_path']}/{self.name}/{t}/"
           tar_file = f"{t}{n}{m}.tar"
           cmd = f"ecp {ecfs_path}{tar_file} ."
           os.system(cmd)
           tar = tarfile.open(tar_file)
           tar.extractall()
           tar.close()

        for f in glob.glob("*.tar.gz"):
           tar = tarfile.open(f)
           tar.extractall()
           tar.close()
        os.system("pwd;ls -lrt ")


#########################################################################
def main(argv):

    parser = ArgumentParser(description="DE_330 case runner")
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        required=True,
        default=None,
        help="Config file for data transfers",
    )

    args = parser.parse_args()
    with open(args.config, "rb") as config_file:
        config = yaml.safe_load(config_file)
    print(config)

    for exp in config["experiments"]:
        e = Exp(exp, config["global"])
        e.prepare_data()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
