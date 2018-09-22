import configparser
import hashlib
from abc import ABCMeta, abstractmethod
from glob import iglob
from os import chdir, environ, mkdir, path, stat

import boto3
from tqdm import tqdm


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
    def push(self, filename, check=False):
        """Push a file on remote repo."""
        pass

    @abstractmethod
    def pull(self, filename, check=False):
        """Pull a file on remote repo."""
        pass

    def list_local(self):
        """List the local files."""
        print("-"*42)
        print("| LOCAL FILES")
        print("-"*42)
        print("| [Size (MB)]-> Filename")
        print("-"*42)
        for file_ in iglob(path.join(self._source, "*"), recursive=True):
            print("| [{:0.2f}]-> {}".format((path.getsize(file_) /
                                             1024) / 1024, file_))
        print("-"*42)

    @abstractmethod
    def list_remote(self):
        """List the remote files."""
        pass

    def configure(self, *args, **kwargs):
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

        for key, value in kwargs.items():
            manager_config['default'][key] = value

        with open(self.__config_file_name, "w") as manager_config_file:
            manager_config.write(manager_config_file)


class S3Manager(Manager):

    def __init__(self, source=None, target=None, config_file_name='manager.ini'):
        super().__init__(source, target, config_file_name)
        self.__s3 = boto3.resource('s3')
        self.__bucket = self._config['default']['bucket']

    def push(self, file_names, check=True):
        """Push a file on remote repo."""
        BASE_FOLDER = path.abspath(self._source)
        chdir(BASE_FOLDER)
        print("-"*42)
        print("| PUSH FILES TO REMOTE STORAGE")
        print("-"*42)
        for filename in file_names:
            for file_ in iglob(path.join(BASE_FOLDER, filename), recursive=True):
                current_file_name = path.join(*path.split(file_)[1:])
                size = stat(file_).st_size
                file_md5 = hashlib.md5()
                with open(file_, "rb") as current_file:
                    file_md5.update(current_file.read())
                md5_digest = file_md5.hexdigest()
                if check:
                    obj = self.__s3.Object(self.__bucket, path.join(self._target, current_file_name))
                    if 'md5' in obj.metadata:
                        if obj.metadata['md5'] == md5_digest:
                            print("[SKIPPED] File '{}' already uploaded and not changed...".format(current_file_name))
                            continue
                    ## TO DO
                    # Check with E_TAG
                pbar = tqdm(desc="Upload {}".format(file_),
                            total=size, unit="bytes", unit_scale=True)
                self.__s3.Bucket(self.__bucket).upload_file(file_, str(
                    path.join(self._target, current_file_name)), ExtraArgs={
                        "Metadata": {"md5": md5_digest}
                }, Callback=pbar.update)
                print(md5_digest)
                pbar.close()
        print("-"*42)

    def pull(self, filename, check=False):
        """Pull a file on remote repo."""
        pass

    def list_remote(self):
        """List the remote files."""
        print("-"*42)
        print("| LOCAL FILES")
        print("-"*42)
        print("| [Size (MB)]-> Filename")
        print("-"*42)
        for obj in self.__s3.Bucket(self.__bucket).objects.filter(Prefix=self._target):
            print("| [{:0.2f}]-> {}".format((obj.meta.data["Size"] /
                                             1024) / 1024, obj.key)[1:])
            # print(obj.Object().metadata['md5'])
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

        super().configure('bucket', engine="s3")
