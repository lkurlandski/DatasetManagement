from pathlib import Path
import re


def clean_paper_collection():
    def rename(stem: str) -> str:
        new_stem = stem.replace("_", " ").replace("-", " ")
        new_stem = "_".join(
            [w[0].upper() + w[1:] for w in new_stem.split()]
        )
        new_stem = re.sub(r'\W+', '', new_stem)
        return new_stem

    path = Path("/home/lk3591/Documents/research/papers")
    for f in path.iterdir():
        if f.name[0] == ".":
            f.unlink()
            continue
        new_f = f.with_stem(rename(f.stem))
        f.replace(new_f)


if __name__ == '__main__':
    clean_paper_collection()
