from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATH = PROJECT_ROOT / 'reddit'
OUTPUT_PATH = PROJECT_ROOT / 'reddit_processed'

__all__ = [
    'PROJECT_ROOT',
    'DATA_PATH',
    'OUTPUT_PATH'
]


if __name__ == '__main__':
    print(f"Config file location: {Path(__file__)}")
    print(f"PROJECT_ROOT points to: {PROJECT_ROOT.resolve()}")
    print(f"PROJECT_ROOT contents: {list(PROJECT_ROOT.iterdir())}")