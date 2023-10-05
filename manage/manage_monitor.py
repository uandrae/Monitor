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
from pathlib import Path


DATESTRING="%Y-%m-%dT%H:%M:%SZ"

class Exp():
    def __init__(self, name, config):

        self.name = name
        self.cfg = config
        self.toc_file = f"{name}_toc.json"
        self.toc = { "vfld" : {}, "done" : []}
        if os.path.isfile(self.toc_file):
           with open(self.toc_file, "r") as infile:
             self.toc = json.load(infile)
             infile.close()
           self.toc["done"] = [datetime.strptime(x, DATESTRING) for x in self.toc["done"]]
        self.toc["vfld"] = self._ecfs_scan(f"{self.cfg['ecfs_path']}/{name}/vfld")
        self.basedir = Path.cwd()

    def _ecfs_scan(self,path):

      cmd = subprocess.Popen(["els", path], stdout=subprocess.PIPE)
      cmd_out, cmd_err = cmd.communicate()
  
      # Decode and filter output
      res = [line.decode("utf-8").replace(f"vfld{self.name}","") for line in cmd_out.splitlines()]
      res = [x.replace(f".tar","") for x in res]
      res = [datetime.strptime(x, "%Y%m") for x in res]

      return res

    def prepare_data(self,month=None,untar=True):

        print("Scan for missing dates")
        if month is None:
            for date in self.toc["vfld"]:
                if date in self.toc["done"]:
                    print(" done:", date)
                else:
                    month = date
                    break

        self.month = month
        if self.month is None:
            return

        print("Process:", month)
        m = month.strftime("%Y%m")
        ym = month.strftime("%Y/%m")
        data_path = f"{self.cfg['data_path']}/{ym}".replace("user",os.environ["USER"])
        os.makedirs(data_path, exist_ok=True)
        os.chdir(data_path)
        for t,n in {"vfld" : self.name ,"vobs": ""}.items():
           ecfs_path = f"{self.cfg['ecfs_path']}/{self.name}/{t}/"
           tar_file = f"{t}{n}{m}.tar"
           cmd = f"ecp {ecfs_path}{tar_file} {data_path}/."
           print(cmd)
           os.system(cmd)
           if untar:
             print("Untar")
             tar = tarfile.open(tar_file)
             tar.extractall()
             tar.close()

        for f in glob.glob("*.tar.gz"):
           if untar:
             tar = tarfile.open(f)
             tar.extractall()
             tar.close()

        os.chdir(self.basedir)

#########################################################################

    def run(self, dry_run=False):

        if self.month is None:
            return

        next_month = self.month + timedelta(days=31)
        edate = next_month - timedelta(days=next_month.day)
        settings = { 
                "CERRA_TU_WEBGRAF_BASE": self.cfg["webgraf_path"].replace("user",os.environ["USER"]),
                "SDATE": self.month.strftime("%Y%m%d"),
                "EDATE": edate.strftime("%Y%m%d"),
                "EXP": self.name,
                "PROJECT": self.cfg["project"],
                "CLEAN_OLD_EXP": "yes" if self.cfg["clean_old_exp"] else "no",
                }
        for k, v in settings.items():
            os.environ[k]=v
            print(f" {k}={v}")

        os.chdir(self.cfg['monitor_path'])
        cmd = f"./Run_verobs_all ./Env_exp"
        print(cmd)
        if not dry_run:
          os.system(cmd)
        os.chdir(self.basedir)
        self.toc["done"].append(self.month)

#########################################################################

    def update_history(self):

       for t in ["vfld", "done"]:
         self.toc[t] = [x.strftime(DATESTRING) for x in sorted(self.toc[t])]
       with open(self.toc_file, "w") as outfile:
         json.dump(self.toc,outfile,indent=1)
       outfile.close()

#########################################################################
def main(argv):

    parser = ArgumentParser(description="CERRA_TU verification manager")
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

    for exp in config["experiments"]:
        e = Exp(exp, config["global"])
        e.prepare_data()
        e.run()
        e.update_history()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
