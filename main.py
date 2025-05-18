import sys
import argparse
from dotenv import dotenv_values
from services.indexer.indexer import run_indexer
from services.spider.crawler import run_crawler

def main():
    config = dotenv_values(".env")
    required = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing = [k for k in required if k not in config]
    if missing:
        print(f"[ERROR]: Missing {', '.join(missing)} in .env file")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Run parts of the Search Engine project.")
    parser.add_argument('task', choices=['indexer', 'crawler'], help='Task to run')
    args = parser.parse_args()

    params = (config['DB_HOST'], config['DB_USER'], config['DB_PASSWORD'], config['DB_NAME'])
    if args.task == 'indexer':
        run_indexer(*params)
    else:
        run_crawler(*params)

if __name__ == "__main__":
    main()