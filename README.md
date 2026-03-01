<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/51214078-61b3-4afe-8add-7df04a34ae54" width="700">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/379c90ea-03cd-4062-a57d-d5da6fc2689f" width="700">
  <img alt="Fallback image description" src="https://github.com/user-attachments/assets/379c90ea-03cd-4062-a57d-d5da6fc2689f" width="700">
</picture>
</p>

<p align="center">
File system snapshot tool that prioritizes snapshot speed and reducing redundant storage.
</p>
<br/>

## How it works

`cacheback` achieves its goals of quick snapshots and minimized snapshot storage size by using hardlink features of modern filesystems
for files whose contents are unchanged between snapshots.
This is similar to how git tracks objects in a repository by storing a file's data based on its content hash.
To further improve speed, a cache of the previous snapshot scan is stored which stores each file's last modification timestamp and
these timestamps are compared before computing the file content hash. If the timestamp is unchanged, it is assumed that the file has 
not changed since the previous snapshot and is linked to the existing content stored on disk.

Here is a diagram visualizing this concept of files within snapshots being pointers to stored data based on content hash:

<p align="center">
  <picture width="600">
    <source
      media="(prefers-color-scheme: light)"
      srcset="https://github.com/user-attachments/assets/4f99e5f0-1aef-48f4-a3cb-960a469353f7"
    >
    <source
      media="(prefers-color-scheme: dark)"
      srcset="https://github.com/user-attachments/assets/05c41fb1-f8a0-4465-8d9b-0f30374317d3"
    >
    <img src="https://github.com/user-attachments/assets/4f99e5f0-1aef-48f4-a3cb-960a469353f7">
  </picture>
</p>

If a file is unchanged between multiple snapshots, each file will point to the same hash-named object and therefore the literal file content
is only stored on disk one time. If snapshots are deleted and a given hashed content is no longer pointed to by any files in any snapshots,
then the `--gargbage-collect` flag will prompt `cacheback` to purge these unused hash-named files to recover storage space.

## Install

## PyPi
```
pip install cacheback-snapshot
```

## ZipApp
Create an executable single-file using python's zipapp feature by running `./scripts/install-zipapp.sh <install_dir>`, for example:

```console
$ ./scripts/install-zipapp.sh ~/.local/bin/
Processing ./.
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
  Preparing metadata (pyproject.toml) ... done
Building wheels for collected packages: cacheback-snapshot
  Building wheel for cacheback-snapshot (pyproject.toml) ... done
Successfully built cacheback-snapshot
Installing collected packages: cacheback-snapshot
Successfully installed cacheback-snapshot

$ ~/.local/bin/cacheback --help
usage: python /home/m/.local/bin/cacheback ...
```

## Usage
>[!NOTE]
> The first time taking a snapshot will take much longer than subsequent snapshots, since the first run will need to copy any and all files to the snapshot storage directory. The real magic of this tool happens on the *subsequent* snapshots that target mostly the same directories.

Installing the package will add an entrypoint executable `cacheback` to your configured executables directory. Run `cacheback --help` for detailed usage information.

### Sample
This example will create a snapshot in the directory in /archives/my-snapshot of the contents of /home/ and /opt/ on the current machine, and will omit including any directories or files that have the word "cache" in them. It will use 2 threads to scan over the directory hierarchies and compute hashes.

```sh
cacheback \
  --snapshot-name 'my-snapshot' \
  --destination /archives/ \
  --targets /home /opt \
  --exclude '**/*cache*' \
  --threads 2
```
