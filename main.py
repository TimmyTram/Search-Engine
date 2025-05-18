import sys
import os
import argparse

# Ensure the root project path is in sys.path
sys.path.append(os.path.dirname(__file__))

from services.indexer.indexer import run_indexer
from services.spider.crawler import run_crawler

def main():
    parser = argparse.ArgumentParser(description="Run parts of the CRAWLER project.")
    parser.add_argument(
        'task',
        choices=['indexer', 'crawler'],
        help='Specify which task to run: indexer or crawler'
    )

    args = parser.parse_args()

    if args.task == 'indexer':
        run_indexer()
    elif args.task == 'crawler':
        run_crawler()

if __name__ == "__main__":
    main()
