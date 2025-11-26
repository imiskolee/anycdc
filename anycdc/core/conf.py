import os
import logging
import yaml


class Config:
    def __init__(self, conf_dir):
        self.conf_dir = conf_dir
        self.connectors = []
        self.tasks = []

    def reload(self):
        self.load_connectors()
        self.load_tasks()

    def load_connectors(self):
        self.tasks = []
        path = os.path.join(self.conf_dir, 'connectors.yaml')
        if not os.path.isfile(path):
            raise Exception("[Config.load_connectors]: connectors.yaml have not found in your root conf dir.")
        with open(path, 'r') as f:
            data = yaml.safe_load_all(f)
            if "connectors" not in data:
                raise Exception(
                    "[Config.load_connectors]: can not found root key `connectors` on your connectors.yaml file")
            self.connectors = data

    def load_tasks(self):
        self.tasks = []
        path = os.path.join(self.conf_dir, 'tasks')
        if not os.path.isdir(path):
            raise Exception("[Config.load_tasks] can not found folder tasks on your root conf dir.")
        for dirpath, _, filenames in os.walk(path, topdown=True):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                with open(file_path, 'r') as f:
                    data = yaml.safe_load_all(f)
                    self.tasks.append(data)
