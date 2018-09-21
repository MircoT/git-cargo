import argparse
import configparser
from os import path

from .storage import S3Manager


def get_engine(config_file_name="manager.ini"):
    config = configparser.ConfigParser()

    if config_file_name and path.isfile(config_file_name):
        config.read(config_file_name)
        if config['default']['engine'] == "s3":
            return S3Manager()
        else:
            raise Exception("Engine not set or unrecognized...")


def main():
    parser = argparse.ArgumentParser(
        prog='cargo', argument_default=argparse.SUPPRESS)

    ##
    # cargo sub_command
    subparsers = parser.add_subparsers(
        help='Cluster setup commands', dest="sub_command")

    ##
    # sub_command configure
    parser_configure = subparsers.add_parser(
        'configure', help='Configure the cargo environment')
    parser_configure.add_argument(
        'configure_target', metavar="target", default="s3", choices=['s3'],
        type=str, help='Environment to configure.')

    ##
    # sub_command ls
    parser_ls = subparsers.add_parser(
        'ls', help='List the cargo source or remote folder')
    parser_ls.add_argument(
        'ls_target', metavar="target", default="local", choices=['remote', 'local'],
        type=str, help='Target to list.')

    ##
    # sub_command push
    parser_push = subparsers.add_parser(
        'push', help='Push files to remote storage')
    parser_push.add_argument('push_target', metavar="target", nargs="+",
                             default=None, type=str, help='Push files to remote storage.')

    ##
    # sub_command pull
    parser_pull = subparsers.add_parser(
        'pull', help='Pull files to local storage')
    parser_pull.add_argument('pull_target', metavar="target", nargs="+",
                             default=None, type=str, help='Pull files to local storage.')

    args, _ = parser.parse_known_args()

    ##
    # OUTPUT TEST - to be removed...
    print(args)

    if args.sub_command == 'configure':
        if args.configure_target == 's3':
            S3Manager().configure()
    elif args.sub_command == 'ls':
        if args.ls_target == 'local':
            S3Manager().list_local()
        elif args.ls_target == 'remote':
            S3Manager().list_remote()
    elif args.sub_command == 'push':
        engine = get_engine()
        engine.push(args.push_target)
    else:
        parser.print_usage()


if __name__ == '__main__':
    main()
