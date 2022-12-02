# Copyright (c) 2022 Jianbin Chang

import argparse
import json

from pyspark.sql import SparkSession


def parse_submissions(j):
    return dict((k, j[k]) for k in ("id", "permalink", "subreddit", "title", "selftext", "score"))


def parse_comments(j):
    return dict((k, j[k]) for k in ("id", "body", "author", "link_id", "parent_id", "score"))


def generate_examples(submission, comments):
    # build comments map
    id_to_comment = {}
    for comment in comments:
        id_to_comment[comment["id"]] = comment

    # collect last comments
    last_comments = []
    ref_count = {}
    for comment in comments:
        parent_id = comment["parent_id"].split("_")[1]
        ref_count[parent_id] = ref_count.get(parent_id, 0) + 1
    for comment in comments:
        id = comment["id"]
        if id not in ref_count:
            last_comments.append(comment)

    examples = []
    for comment in last_comments:
        example = [comment]
        parent_id = comment["parent_id"].split("_")[1]
        while parent_id in id_to_comment:
            comment = id_to_comment[parent_id]
            parent_id = comment["parent_id"].split("_")[1]
            example.append(comment)

        example.reverse()
        examples.append(example)

    return (submission, examples)


def create_data(args):
    if args.spark_archives:
        spark = SparkSession.builder.config("spark.archives", args.spark_archives)\
            .master(args.spark_master)\
            .getOrCreate()
    else:
        spark = SparkSession.builder.master(args.spark_master).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel(args.spark_log_level)

    submissions = sc.textFile(args.input_submissions_path)\
        .map(lambda x: parse_submissions(json.loads(x)))
    comments = sc.textFile(args.input_comments_path)\
        .map(lambda x: parse_comments(json.loads(x)))

    threads = comments.map(lambda x: (x["link_id"].split("_")[1], x))\
        .groupByKey()\
        .map(lambda x: (x[0], list(x[1])))
    examples = submissions.map(lambda x: (x["id"], x))\
        .join(threads)

    examples = examples.map(lambda x: json.dumps(x))

    return examples


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--spark_master", default="local[*]")
    parser.add_argument("--spark_archives", default=None,
                        help="https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html#using-conda")
    parser.add_argument("--input_submissions_path", required=True)
    parser.add_argument("--input_comments_path", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--spark_log_level", default="ERROR", choices=["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"])

    args = parser.parse_args()

    return args


def main():
    args = parse_args()

    reddit_topical_chat = create_data(args)
    reddit_topical_chat.saveAsTextFile(args.output_dir)


if __name__ == "__main__":
    main()
