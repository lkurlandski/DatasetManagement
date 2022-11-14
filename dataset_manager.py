from abc import ABC, abstractmethod
from argparse import ArgumentParser
import hashlib
from pathlib import Path
import shutil
import subprocess
import zipfile
import zlib

from sklearn.model_selection import train_test_split
from tqdm import tqdm


random_state = 0
datasets_path = Path("/home/lk3591/Documents/datasets")


class DatasetManager(ABC):

    def __init__(self, name: str, *args, **kwargs):
        self.name = name
        self.root = datasets_path / self.name
        self.downloads_path = self.root / "downloads"
        self.extracted_path = self.root / "extracted"
        self.processed_path = self.root / "processed"

    def _setup(self):
        for p in (self.root, self.downloads_path, self.extracted_path, self.processed_path):
            p.mkdir(exist_ok=True, parents=True)

    def _clean(self, remove_download: bool):
        paths = [self.extracted_path, self.processed_path]
        if remove_download:
            paths.append(self.downloads_path)
        for path in paths:
            shutil.rmtree(path)

    def _process_into_train_test_split(self):
        # FIXME: use symbolic links instead of a copy
        (self.processed_path / "train").mkdir(exist_ok=True)
        (self.processed_path / "test").mkdir(exist_ok=True)
        train, test = train_test_split([f for f in self.extracted_path.iterdir()])
        for f_train, f_test in zip(train, test):
            shutil.copy2(f_train, (self.processed_path / "train"))
            shutil.copy2(f_test, (self.processed_path / "test"))

    def clean(self):
        raise NotImplementedError(f"{self.name} cannot be cleaned.")

    def download(self):
        raise NotImplementedError(f"{self.name} cannot be downloaded.")

    def extract(self):
        raise NotImplementedError(f"{self.name} cannot be extracted.")

    def process(self):
        raise NotImplementedError(f"{self.name} cannot be processed.")


class Sorel(DatasetManager):
    
    def __init__(self, *args, **kwargs):
        super().__init__("Sorel", * args, **kwargs)

    def clean(self):
        super(Sorel, self)._clean(remove_download=True)

    def download(self):
        subprocess.run(
            [
                "aws",
                "--no-sign-request",
                "s3",
                "cp",
                "s3://sorel-20m/09-DEC-2020/binaries/",
                f"{self.downloads_path}",
                "--recursive",
                "--exclude='*'",
                "--include='00*'"
            ],
        )

    def extract(self):
        for f in tqdm(list(self.downloads_path.iterdir())):
            with open(f, "rb") as file:
                decompressed = zlib.decompress(file.read())
            with open(self.extracted_path / f.name, "wb") as file:
                file.write(decompressed)

    def process(self):
        self._process_into_train_test_split()

class Windows(DatasetManager):

    def __init__(self, *args, **kwargs):
        super().__init__("Windows", *args, **kwargs)

    def clean(self):
        super().clean()

    def download(self):
        # Go on to a Windows machine and enter > robocopy C:\ Windows *.exe /S
        super().download()

    def extract(self):
        self._setup()
        for p in self.downloads_path.rglob("*.exe"):
            local_path = p.as_posix().replace(self.extracted_path.as_posix(), "")
            hash = hashlib.sha224(bytes(local_path[1:], "utf-8")).hexdigest()
            p.replace((self.extracted_path / hash).with_suffix(".exe"))
        for p in self.downloads_path.iterdir():
            shutil.rmtree(p)

    def process(self):
        self._process_into_train_test_split()

class VirusShare(DatasetManager):

    def __init__(self, version:str, *args, **kwargs):
        self.version = version
        super().__init__("VirusShare", *args, **kwargs)
        self.extracted_path = self.extracted_path / self.version

    def clean(self):
        super()._clean(remove_download=False)

    def extract(self):
        file = self.downloads_path / f"VirusShare_{self.version}.zip"
        self.extracted_path.mkdir(exist_ok=True, parents=True)
        raise NotImplementedError("FIXME: hangs after 43817 files, when should be 65536 files")
        # FIXME: hangs after 43817 files, when should be 65536 files
        with zipfile.ZipFile(file, 'r') as zip_ref:
            zip_ref.extractall(self.extracted_path, pwd=bytes("infected", "utf-8"))

    def process(self):
        self._process_into_train_test_split()


class Sleipnir(DatasetManager):

    def __init__(self, *args, **kwargs):
        super().__init__("SLEIPNIR", *args, **kwargs)

    def clean(self):
        super()._clean(remove_download=False)

    def download(self):
        raise NotImplementedError(f"{self.name} not available for download.")

    def extract(self):
        # TODO: implement
        raise NotImplementedError()

    def process(self):
        # TODO: change the file extension to .pt since they are tensors
        # Create the processed directories
        for split in ("train", "test"):
            (self.processed_path / split).mkdir(exist_ok=True)
            for category in ("benign", "malicious"):
                (self.processed_path / split / category).mkdir(exist_ok=True)
        # Copy to processed
        for category in ("benign", "malicious"):
            train, test = train_test_split([f for f in (self.extracted_path / category).iterdir()])
            for f_train, f_test in zip(train, test):
                shutil.copy2(f_train, self.processed_path / "train" / category)
                shutil.copy2(f_test, self.processed_path / "test" / category)


def main(dataset:str, clean:bool, download:bool, extract:bool, process:bool, version:str=None):
    name_to_manager_map = {
        "SLEIPNIR": Sleipnir,
        "VirusShare": VirusShare,
        "Sorel": Sorel,
        "Windows": Windows,
    }
    if dataset not in name_to_manager_map:
        raise ValueError(f"Invalid dataset provided: {dataset}")
    manager = name_to_manager_map[dataset](version=version)
    if clean:
        manager.clean()
    if download:
        manager.download()
    if extract:
        manager.extract()
    if process:
        manager.process()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--dataset", help="Name of the dataset to manage.")
    parser.add_argument("--version", required=False, help="Optional version of the dataset.")
    parser.add_argument("--clean", action="store_true", default=False, help="Remove all non-essential data.")
    parser.add_argument("--download", action="store_true", default=False, help="Download dataset from source.")
    parser.add_argument("--extract", action="store_true", default=False, help="Extract dataset from download.")
    parser.add_argument("--process", action="store_true", default=False, help="Process the extracted dataset.")
    args = parser.parse_args()
    main(args.dataset, args.clean, args.download, args.extract, args.process, args.version)
