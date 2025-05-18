import sys
import argparse
from dotenv import dotenv_values
from services.indexer.indexer import run_indexer
from services.spider.crawler import run_crawler

def main():
    config = dotenv_values(".env")
    required = ['host', 'user', 'password', 'database']
    missing = [k for k in required if k not in config]
    if missing:
        print(f"[ERROR]: Missing {', '.join(missing)} in .env file")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Run parts of the CRAWLER project.")
    parser.add_argument('task', choices=['indexer', 'crawler'], help='Task to run')
    args = parser.parse_args()

    params = (config['host'], config['user'], config['password'], config['database'])
    if args.task == 'indexer':
        run_indexer(*params)
    else:
        run_crawler(*params)

if __name__ == "__main__":
    main()
