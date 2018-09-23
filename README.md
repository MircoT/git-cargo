# git-cargo
An easy external storage manager for git binary and big files

## Getting Started

These module helps you to manage date when you exceeded the limit of `git lfs` and one needs to rely on external storage.

## Prerequisites

You need Python 3 installed in your system.

## Install

Use the setup.py file:

```bash
python setup.py install
```

After that, you can access to the module using `python -m cargo` or with `git cargo` command.

## How to use

Cargo supports the following remote storage:

* AWS S3 - to configure use `git cargo configure s3`

Configure the proper storage and then use the pull and push commands as follow:

```bash
git cargo push \*
git cargo pull my_file --force True
```

## Contributing

Please feel free to contribute to this project, make suggestions or adding new remote storages.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/MircoT/git-cargo/tags). 

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
