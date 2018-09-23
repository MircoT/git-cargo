import configparser
import hashlib
from abc import ABCMeta, abstractmethod
from glob import iglob
from math import ceil
from os import chdir, environ, makedirs, mkdir, path, stat

import boto3
import botocore
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
    def push(self, file_name, check=False):
        """Push a file to remote repo."""
        pass

    @abstractmethod
    def pull(self, file_name, check=False):
        """Pull a file from remote repo."""
        pass

    def list_local(self):
        """List the local files."""
        print("-"*42)
        print("| LOCAL FILES")
        print("-"*42)
        print("| [Size (MB)]-> File_name")
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

    def __read_file_by_chuncks(self, filepath, chunk_size=16, desc="Read file"):
        CHUNK_SIZE_MB = chunk_size * 1024 * 1024
        file_name = path.join(*path.split(filepath)[1:])
        size = stat(filepath).st_size
        pbar = tqdm(desc="{} {}".format(desc, file_name),
                    total=size, unit="bytes", unit_scale=True)
        with open(filepath, "rb") as current_file:
            while current_file.tell() != size:
                yield current_file.read(CHUNK_SIZE_MB)
                pbar.update(CHUNK_SIZE_MB)
        pbar.close()

    def __gen_md5(self, filepath, chunk_size=8):
        current_md5 = hashlib.md5()
        for chunk in self.__read_file_by_chuncks(filepath, chunk_size, desc="[Calculation md5]"):
            current_md5.update(chunk)
        return current_md5.hexdigest()

    def __gen_etag(self, filepath, chunk_size=16):
        md5_list = []
        for chunk in self.__read_file_by_chuncks(filepath, chunk_size, desc="[Calculation Etag]"):
            md5_list.append(hashlib.md5(chunk).digest())
        digests = b"".join(md5_list)
        etag_digest = "{}-{}".format(
            hashlib.md5(digests).hexdigest(),
            len(md5_list)
        )
        return etag_digest

    @staticmethod
    def __get_s3obj_etag_nparts(string):
        """Get the number of digest calculated with etag.

        Example:
            "xyz123abc-5" -> "5"
        """
        return string.split("-")[-1].strip()

    def __etag_ok(self, filepath, local_etag, remote_etag):
        if self.__get_s3obj_etag_nparts(local_etag) != self.__get_s3obj_etag_nparts(remote_etag):
            parts = int(self.__get_s3obj_etag_nparts(remote_etag))
            size = stat(filepath).st_size
            chunk_size = ceil((size / parts) / 1024 / 1024)
            local_etag = self.__gen_etag(filepath, chunk_size)

        return local_etag == remote_etag

    @staticmethod
    def __get_s3obj_etag(s3object):
        return s3object.meta.data['ETag'].replace("\"", "")
    
    @staticmethod
    def __get_s3obj_size(s3object):
        return s3object.meta.data['Size']
    
    def __get_relative_path(self, current_path, target='local'):
        tail, head = path.split(current_path)
        tmp = [head]
        if target == 'local':
            target_folder = path.split(self._target)[1]
        elif target == 'remote':
            target_folder = path.split(self._source)[1]
        else:
            raise Exception("Not a valid target")
        while head != target_folder:
            tail, head = path.split(tail)
            tmp.append(head)
        if target == 'local':
            tmp.pop(-1)
        elif target == 'remote':
            tmp[-1] = path.split(self._target)[1]
        return path.join(*list(reversed(tmp)))

    def push(self, file_names, force=False, chunk_size=16):
        """Push a file to remote repo."""
        BASE_FOLDER = path.abspath(self._source)
        chdir(BASE_FOLDER)
        print("-"*42)
        print("| PUSH FILES TO REMOTE STORAGE")
        print("-"*42)
        for file_name in file_names:
            for file_ in iglob(path.join(BASE_FOLDER, file_name), recursive=True):
                current_file_name = path.split(file_)[1]
                size = stat(file_).st_size
                md5_digest = self.__gen_md5(file_)
                if not force:
                    try:
                        obj = self.__s3.Object(self.__bucket, path.join(
                            self._target, current_file_name))
                        if 'md5' in obj.metadata:
                            if obj.metadata['md5'] == md5_digest:
                                print("[SKIPPED] File '{}' already uploaded and not changed...".format(
                                    current_file_name))
                                continue
                        else:
                            etag_digest = self.__gen_etag(file_, chunk_size)
                            remote_etag = self.__get_s3obj_etag(obj)
                            if self.__etag_ok(file_, etag_digest, remote_etag):
                                print("[SKIPPED] File '{}' already uploaded and not changed...".format(
                                    current_file_name))
                                continue
                            else:
                                raise Exception(
                                    "Can't check file {} on remote storage".format(current_file_name))
                    except botocore.exceptions.ClientError as exp:
                        error_code = exp.response['Error']['Code']
                        if error_code != '404':
                            raise
                pbar = tqdm(desc="Upload {}".format(file_),
                            total=size, unit="bytes", unit_scale=True)
                self.__s3.Bucket(self.__bucket).upload_file(
                    file_,
                    self.__get_relative_path(file_, 'remote'), 
                    ExtraArgs={
                        "Metadata": {"md5": md5_digest}
                    }, 
                    Callback=pbar.update
                )
                pbar.close()
        print("-"*42)

    def pull(self, file_names, force=False, chunk_size=16):
        """Pull a file from remote repo."""
        BASE_FOLDER = path.abspath(self._source)
        for file_name in file_names:
            for obj in self.__s3.Bucket(self.__bucket).objects.filter(Prefix=path.join(self._target, file_name)):
                current_file_name = path.split(obj.key)[1]
                local_file = path.join(BASE_FOLDER, self.__get_relative_path(obj.key, 'local'))
                if not force:
                    if 'md5' in obj.Object().metadata:
                        md5_digest = self.__gen_md5(local_file)
                        if md5_digest != obj.Object().metadata['md5']:
                            print("[SKIPPED] File '{}' already downloaded and not changed...".format(
                                current_file_name))
                            continue
                        else:
                            etag_digest = self.__gen_etag(
                                local_file, chunk_size)
                            remote_etag = self.__get_s3obj_etag(obj)
                            if self.__etag_ok(local_file, etag_digest, remote_etag):
                                print("[SKIPPED] File '{}' already downloaded and not changed...".format(
                                    current_file_name))
                                continue
                            else:
                                raise Exception(
                                    "Can't check file {} from remote storage".format(current_file_name))
                pbar = tqdm(desc="Download {}".format(obj.key),
                            total=self.__get_s3obj_size(obj), unit="bytes", unit_scale=True)
                makedirs(path.split(local_file)[0], exist_ok=True)
                self.__s3.Bucket(self.__bucket).download_file(
                    obj.key, 
                    local_file, 
                    Callback=pbar.update
                )
                pbar.close()

    def list_remote(self):
        """List the remote files."""
        print("-"*42)
        print("| LOCAL FILES")
        print("-"*42)
        print("| [Size (MB)]-> File name")
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
