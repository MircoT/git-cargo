import configparser
from abc import ABCMeta, abstractmethod
from glob import glob
from os import environ, mkdir, path

import boto3


class Manager(metaclass=ABCMeta):

    def __init__(self, source=None, target=None, config_file_name='manager.ini'):
        self.__config_file_name = config_file_name
        self._config = configparser.ConfigParser()

        if config_file_name and path.isfile(config_file_name):
            self._config.read(self.__config_file_name)
            self._source = self._config['default']['source']
            self._target = self._config['default']['target']

        if source:
            self._source = source
        if target:
            self._target = target

    @abstractmethod
    def push(self, filename):
        """Push a file on remote repo."""
        pass

    @abstractmethod
    def pull(self, filename):
        """Pull a file on remote repo."""
        pass

    def list_local(self):
        """List the local files."""
        print("-"*42)
        print("| LOCAL FILES")
        print("-"*42)
        print("| [Size (MB)]-> Filename")
        print("-"*42)
        for file_ in glob(path.join(self._source, "*")):
            print("| [{:0.2f}]-> {}".format((path.getsize(file_) /
                                             1024) / 1024, path.split(file_)[-1]))
        print("-"*42)

    @abstractmethod
    def list_remote(self):
        """List the remote files."""
        pass

    def configure(self, *args):
        """Configure the manager."""
        source = input("Insert your source folder path: ")
        target = input(
            "Insert your target folder name (the remote folder to use): ")

        additional_config = {}
        for key in args:
            additional_config[key] = input(
                "Insert your default {}: ".format(key))

        manager_config = configparser.ConfigParser()
        manager_config['default'] = {
            'source': source,
            'target': target
        }

        for key, value in additional_config.items():
            manager_config['default'][key] = value

        with open(self.__config_file_name, "w") as manager_config_file:
            manager_config.write(manager_config_file)


class S3Manager(Manager):

    def __init__(self, source=None, target=None, config_file_name='manager_s3.ini'):
        super().__init__(source, target, config_file_name)
        self.__s3 = boto3.resource('s3')
        self.__bucket = self._config['default']['bucket']

    def push(self, filename):
        """Push a file on remote repo."""
        pass

    def pull(self, filename):
        """Pull a file on remote repo."""
        pass

    def list_remote(self):
        """List the remote files."""
        print("-"*42)
        print("| LOCAL FILES")
        print("-"*42)
        print("| [Size (MB)]-> Filename")
        print("-"*42)
        for obj in self.__s3.Bucket(self.__bucket).objects.filter(Prefix=self._target, Delimiter="/"):
            print(obj)
        print("-"*42)

    def configure(self):
        """Configure the manager."""

        aws_access_key_id = input("Insert your aws access key id: ")
        aws_secret_access_key = input("Insert your aws secret access key: ")

        aws_credentials = configparser.ConfigParser()
        aws_credentials['default'] = {
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key
        }

        user_home = environ['HOME']
        aws_folder = path.join(user_home, ".aws")
        if not path.isdir(aws_folder):
            mkdir(aws_folder)

        with open(path.join(aws_folder, "credentials"), "w") as aws_credential_file:
            aws_credentials.write(aws_credential_file)

        region = input("Insert your aws region: ")

        aws_config = configparser.ConfigParser()
        aws_config['default'] = {
            'region': region
        }

        with open(path.join(aws_folder, "config"), "w") as aws_config_file:
            aws_config.write(aws_config_file)

        super().configure('bucket')
