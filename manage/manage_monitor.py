#!/usr/bin/env python3

#
# Ulf Andrae, SMHI, 2023
#

import re
import os
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
    def __init__(self, name, local, config):

        print(f"\nCreate setup for {name}\n")

        self.name = name
        self.local = local
        self.cfg = config
        for x,y in self.cfg.items():
            if isinstance(y,str):
              self.cfg[x] = y.replace("user",os.environ["USER"])

        self.cfg["data_path"] += f"/{self.name}"

        self.toc_file = f"data/{name}_toc.json"
        self.toc = { "vfld" : {}, "done" : []}
        if os.path.isfile(self.toc_file):
           with open(self.toc_file, "r") as infile:
             self.toc = json.load(infile)
             infile.close()
           self.toc["done"] = [datetime.strptime(x, DATESTRING) for x in self.toc["done"]]
        self.toc["vfld"] = self._ecfs_scan(f"{self.cfg['ecfs_path']}/{name}/vfld")
        
        self.basedir = Path.cwd()
        self.month = self.check_missing()

#########################################################################

    def mbr(self):
      mbr= ""
      if "mbr" in self.local:
          mbr = f"mbr{self.local['mbr']:03d}"
      return mbr

#########################################################################

    def _ecfs_scan(self,path):

      print("Scan:", path)
      cmd = subprocess.Popen(["els", path], stdout=subprocess.PIPE)
      cmd_out, cmd_err = cmd.communicate()
  
      mbr = self.mbr()
      regex = f"vfld(.*){mbr}([0-9]{{6}})(\.tar)"

      # Decode and filter output
      res = []
      for line in cmd_out.splitlines():
          l = line.decode("utf-8")
          if mbr not in l:
              next
          x = re.fullmatch(regex,l)
          if x is not None:
            x = x.groups()[1] 
            x = datetime.strptime(x, "%Y%m")
            res.append(x) 

      return res

#########################################################################

    def check_missing(self, verbose=False):
        if verbose:
          print("Scan for missing dates")
        month = None
        for date in self.toc["vfld"]:
                if date in self.toc["done"]:
                    if verbose:
                      print(" done:", date)
                else:
                    if verbose:
                      print(" not done:", date)
                    month = date
                    break

        return month

#########################################################################

    def prepare_data(self,month=None,untar=True):

        if month is None:
            month = self.check_missing(verbose=True)
        self.month = month
        if self.month is None:
            return

        print("Process:", month)
        m = self.month.strftime("%Y%m")
        ym = self.month.strftime("%Y/%m")
        data_path = f"{self.cfg['data_path']}/{ym}"
        os.makedirs(data_path, exist_ok=True)
        os.chdir(data_path)
        mbr = self.mbr()
        for t,n in {"vfld" : f"{self.name}{mbr}" ,"vobs": ""}.items():
           ecfs_path = f"{self.cfg['ecfs_path']}/{self.name}/{t}/"
           tar_file = f"{t}{n}{m}.tar"
           cmd = f"ecp {ecfs_path}{tar_file} {data_path}/."
           print(cmd)
           os.system(cmd)
           if untar:
             print("Untar", tar_file)
             tar = tarfile.open(tar_file)
             tar.extractall()
             tar.close()

        for t in [f"vfld{self.name}{mbr}" ,"vobs"]:
         for f in glob.glob(f"{t}*.tar.gz"):
           if untar:
             print("Untar", f)
             tar = tarfile.open(f)
             tar.extractall()
             tar.close()

        os.chdir(self.basedir)

#########################################################################

    def clean_data(self,month=None):
        if month is None:
            month = self.month
        if self.month is None:
            return

        print("Clean:", month)
        m = month.strftime("%Y%m")
        ym = month.strftime("%Y/%m")
        data_path = f"{self.cfg['data_path']}/{ym}"
        os.chdir(data_path)
        for f in glob.glob("*"):
          if "tar" not in f:
              os.remove(f)
          elif "tar.gz" in f:
              os.remove(f)
          else:
              print(" leave:", f)
        os.chdir(self.basedir)



#########################################################################

    def run(self, dry_run=False):

        if self.month is None:
            return

        sdate = self.month.strftime("%Y%m%d")
        if len(self.toc["done"]) > 0:
            idate = list(self.toc["done"])[0].strftime("%Y%m%d")
        else:
            idate = sdate
        next_month = self.month + timedelta(days=31)
        edate = next_month - timedelta(days=next_month.day)
        mbr = self.mbr()
        exp = f"{self.name}{mbr}"
        if "display_exp" in self.local:
            display_exp = self.local["display_exp"]
        else:
            display_exp = exp

        # Copy scripts
        wrk = self.cfg['wrk']
        os.makedirs(wrk, exist_ok=True)
        os.system(f"rsync -aux {self.cfg['monitor_path']}/scr {wrk}/")
        scr = f"{wrk}/scr"

        settings = { 
                "MONITOR_PATH": self.cfg["monitor_path"],
                "CERRA_TU_WEBGRAF_BASE": self.cfg["webgraf_path"],
                "WRK": wrk,
                "BIN": f"{self.cfg['monitor_path']}/bin",
                "SCR": scr,
                "SDATE": sdate,
                "EDATE": edate.strftime("%Y%m%d"),
                "IDATE": idate,
                "EXP": exp,
                "DISPLAY_EXP": display_exp,
                "PROJECT": self.local["project"],
                "CLEAN_OLD_EXP": "yes" if self.cfg["clean_old_exp"] else "no",
                }

        print("Environment variables:")
        for k, v in settings.items():
            os.environ[k]=v
            print(f" {k}={v}")

        os.chdir(scr)
        cmd = f"./Run_verobs_all ./Env_exp"
        print(cmd)
        if not dry_run:
          os.system(cmd)
        os.chdir(self.basedir)
        self.toc["done"].append(self.month)

#########################################################################

    def update_history(self):

       print("Update:", self.toc_file)
       toc = {}
       for t in ["vfld", "done"]:
         toc[t] = [x.strftime(DATESTRING) for x in sorted(self.toc[t])]
       with open(self.toc_file, "w") as outfile:
         json.dump(toc,outfile,indent=1)
       outfile.close()

#########################################################################
def main(argv):

    parser = ArgumentParser(description="CERRA_TU verification manager")
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        default="config.yml",
        help="Config file for verification"
    )

    args = parser.parse_args()
    with open(args.config, "rb") as config_file:
        config = yaml.safe_load(config_file)

    for exp, local in config["experiments"].items():
        e = Exp(exp, local, config["global"])
        i = 1
        while e.month is not None and i <= e.cfg["maxruns"] :
          e.prepare_data(untar=config["global"]["untar"])
          e.run(dry_run=config["global"]["dry_run"])
          #e.clean_data()
          i = i + 1
        e.update_history()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
