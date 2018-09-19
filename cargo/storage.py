import configparser
from abc import ABCMeta, abstractmethod
from os import environ, mkdir, path


class Manager(metaclass=ABCMeta):

    @abstractmethod
    def push(self, filename):
        """Push a file on remote repo."""
        pass

    @abstractmethod
    def pull(self, filename):
        """Pull a file on remote repo."""
        pass

    @abstractmethod
    def list_local(self):
        """List the local files."""
        pass

    @abstractmethod
    def list_remote(self):
        """List the remote files."""
        pass
    
    @abstractmethod
    def configure(self):
        """Configure the manager."""
        pass


class S3Manager(Manager):

    def push(self, filename):
        """Push a file on remote repo."""
        pass

    def pull(self, filename):
        """Pull a file on remote repo."""
        pass

    def list_local(self):
        """List the local files."""
        pass

    def list_remote(self):
        """List the remote files."""
        pass
    
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
