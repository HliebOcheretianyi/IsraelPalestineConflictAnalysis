import zstandard
import os
import json
import sys
import csv
from datetime import datetime
import logging.handlers
import traceback

from paths import *

# put the path to the input file, or a folder of files to process all of
input_file = f"{DATA_PATH}/comments/"

# put the name or path to the output file. The file extension from below will be added automatically. If the input file is a folder, the output will be treated as a folder as well
output_file = f"{OUTPUT_PATH}/comments/"

# the format to output in, pick from the following options
#   zst: same as the input, a zstandard compressed ndjson file. Can be read by the other scripts in the repo
#   txt: an ndjson file, which is a text file with a separate json object on each line. Can be opened by any text editor
#   csv: a comma separated value file. Can be opened by a text editor or excel
output_format = "csv"

# override the above format and output only this field into a text file, one per line. Useful if you want to make a list of authors or ids.
single_field = None
write_bad_lines = True

# if you want to output only specific columns for CSV format, list them here. If empty, all available fields are included
# For submissions: ["author","subreddit","title","num_comments","score","over_18","created","link","text","url"]
# For comments: ["author","subreddit","score","created","link","body"]
csv_fields = ["author", "subreddit", "score", "created", "link", "body"]

# only output items between these two dates
from_date = datetime.strptime("2005-01-01", "%Y-%m-%d")
to_date = datetime.strptime("2030-12-31", "%Y-%m-%d")

# FILTER 1: Include only these subreddits
field1 = "subreddit"
values1 = [
    "Palestine",
    "palestinenews",
    "Israel",
    "IsraelPalestine",
    "IsraelCrimes",
    "israelexposed",
    "Lebanon",
    "worldnews",
    "AskMiddleEast"
]
exact_match1 = True
inverse1 = False

# FILTER 2: Exclude authors in ignore.txt
field2 = "author"
values2 = []
values_file2 = "ignored.txt"
exact_match2 = True
inverse2 = True  # Set to True to EXCLUDE matching authors

# sets up logging to the console as well as a file
log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
log_str_handler = logging.StreamHandler()
log_str_handler.setFormatter(log_formatter)
log.addHandler(log_str_handler)
if not os.path.exists("logs"):
    os.makedirs("logs")
log_file_handler = logging.handlers.RotatingFileHandler(os.path.join("logs", "bot.log"), maxBytes=1024 * 1024 * 16,
                                                        backupCount=5)
log_file_handler.setFormatter(log_formatter)
log.addHandler(log_file_handler)


def write_line_zst(handle, line):
    handle.write(line.encode('utf-8'))
    handle.write("\n".encode('utf-8'))


def write_line_json(handle, obj):
    handle.write(json.dumps(obj))
    handle.write("\n")


def write_line_single(handle, obj, field):
    if field in obj:
        handle.write(obj[field])
    else:
        log.info(f"{field} not in object {obj['id']}")
    handle.write("\n")


def write_line_csv(writer, obj, is_submission, fields=None):
    output_list = []

    # If specific fields requested, use them in order
    if fields is not None and len(fields) > 0:
        # Extract fields in the order specified
        for field in fields:
            if field == "created":
                value = datetime.fromtimestamp(int(obj['created_utc'])).strftime("%Y-%m-%d %H:%M")
            elif field == "link":
                if 'permalink' in obj:
                    value = f"https://www.reddit.com{obj['permalink']}"
                else:
                    value = f"https://www.reddit.com/r/{obj['subreddit']}/comments/{obj['link_id'][3:]}/_/{obj['id']}/"
            elif field == "author":
                value = f"u/{obj['author']}"
            elif field == "text":
                if 'selftext' in obj:
                    value = obj['selftext']
                else:
                    value = ""
            else:
                value = obj.get(field, "")

            output_list.append(str(value).encode("utf-8", errors='replace').decode())
    else:
        # Use default set based on submission/comment type
        if is_submission:
            default_fields = ["author", "subreddit", "title", "num_comments", "score", "over_18", "created", "link",
                              "text", "url"]
        else:
            default_fields = ["author", "subreddit", "score", "created", "link", "body"]

        for field in default_fields:
            if field == "created":
                value = datetime.fromtimestamp(int(obj['created_utc'])).strftime("%Y-%m-%d %H:%M")
            elif field == "link":
                if 'permalink' in obj:
                    value = f"https://www.reddit.com{obj['permalink']}"
                else:
                    value = f"https://www.reddit.com/r/{obj['subreddit']}/comments/{obj['link_id'][3:]}/_/{obj['id']}/"
            elif field == "author":
                value = f"u/{obj['author']}"
            elif field == "text":
                if 'selftext' in obj:
                    value = obj['selftext']
                else:
                    value = ""
            else:
                value = obj.get(field, "")

            output_list.append(str(value).encode("utf-8", errors='replace').decode())

    writer.writerow(output_list)


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
        log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
    with open(file_name, 'rb') as file_handle:
        buffer = ''
        reader = zstandard.ZstdDecompressor(max_window_size=2 ** 31).stream_reader(file_handle)
        while True:
            chunk = read_and_decode(reader, 2 ** 27, (2 ** 29) * 2)

            if not chunk:
                break
            lines = (buffer + chunk).split("\n")

            for line in lines[:-1]:
                yield line.strip(), file_handle.tell()

            buffer = lines[-1]

        reader.close()


def check_filter(obj, field, values, exact_match, inverse):
    """Check if an object matches a filter. Returns True if it should be included."""
    if field is None:
        return True

    try:
        field_value = obj[field]
        if field_value is None:
            return False
        field_value = field_value.lower()
        matched = False
        for value in values:
            if exact_match:
                if value == field_value:
                    matched = True
                    break
            else:
                if value in field_value:
                    matched = True
                    break
        if inverse:
            return not matched
        else:
            return matched
    except KeyError:
        return False


def process_file(input_file, output_file, output_format, field1, values1, exact_match1, inverse1, field2, values2,
                 exact_match2, inverse2, from_date, to_date, single_field, csv_fields):
    output_path = f"{output_file}.{output_format}"
    is_submission = "submission" in input_file
    log.info(f"Input: {input_file} : Output: {output_path} : Is submission {is_submission}")
    writer = None
    if output_format == "zst":
        handle = zstandard.ZstdCompressor().stream_writer(open(output_path, 'wb'))
    elif output_format == "txt":
        handle = open(output_path, 'w', encoding='UTF-8')
    elif output_format == "csv":
        handle = open(output_path, 'w', encoding='UTF-8', newline='')
        writer = csv.writer(handle)
        # Write header row
        if csv_fields and len(csv_fields) > 0:
            writer.writerow(csv_fields)
        else:
            # Write default headers based on submission/comment type
            if is_submission:
                writer.writerow(
                    ["author", "subreddit", "title", "num_comments", "score", "over_18", "created", "link", "text",
                     "url"])
            else:
                writer.writerow(["author", "subreddit",'controversiality', "score", "created", "link", "body"])
    else:
        log.error(f"Unsupported output format {output_format}")
        sys.exit()

    file_size = os.stat(input_file).st_size
    created = None
    matched_lines = 0
    bad_lines = 0
    total_lines = 0
    for line, file_bytes_processed in read_lines_zst(input_file):
        total_lines += 1
        if total_lines % 100000 == 0:
            log.info(
                f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {total_lines:,} : {matched_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")

        try:
            obj = json.loads(line)
            created = datetime.utcfromtimestamp(int(obj['created_utc']))

            if created < from_date:
                continue
            if created > to_date:
                continue

            # Apply both filters
            if not check_filter(obj, field1, values1, exact_match1, inverse1):
                continue
            if not check_filter(obj, field2, values2, exact_match2, inverse2):
                continue

            matched_lines += 1
            if output_format == "zst":
                write_line_zst(handle, line)
            elif output_format == "csv":
                write_line_csv(writer, obj, is_submission, csv_fields)
            elif output_format == "txt":
                if single_field is not None:
                    write_line_single(handle, obj, single_field)
                else:
                    write_line_json(handle, obj)
            else:
                log.info(f"Something went wrong, invalid output format {output_format}")
        except (KeyError, json.JSONDecodeError) as err:
            bad_lines += 1
            if write_bad_lines:
                if isinstance(err, KeyError):
                    log.warning(f"Key error: {err}")
                elif isinstance(err, json.JSONDecodeError):
                    log.warning(f"Line decoding failed: {err}")

    handle.close()
    log.info(f"Complete : {total_lines:,} : {matched_lines:,} : {bad_lines:,}")


if __name__ == "__main__":
    if single_field is not None:
        log.info("Single field output mode, changing output file format to txt")
        output_format = "txt"

    # Load values for filter 2 from file if specified
    if values_file2:
        values2 = []
        with open(values_file2, 'r') as values_handle:
            for value in values_handle:
                values2.append(value.strip().lower())
        log.info(f"Loaded {len(values2)} authors from {values_file2}")
    else:
        values2 = [value.lower() for value in values2]

    # Convert filter 1 values to lowercase
    values1 = [value.lower() for value in values1]

    log.info(f"Filter 1 - Field: {field1}, Exact match: {exact_match1}, Inverse: {inverse1}")
    if len(values1) <= 20:
        log.info(f"  Values: {','.join(values1)}")

    log.info(f"Filter 2 - Field: {field2}, Exact match: {exact_match2}, Inverse: {inverse2}")
    if len(values2) <= 20:
        log.info(f"  Values: {','.join(values2)}")

    log.info(f"From date {from_date.strftime('%Y-%m-%d')} to date {to_date.strftime('%Y-%m-%d')}")
    log.info(f"Output format set to {output_format}")

    input_files = []
    if os.path.isdir(input_file):
        if not os.path.exists(output_file):
            os.makedirs(output_file)
        for file in os.listdir(input_file):
            if not os.path.isdir(file) and file.endswith(".zst"):
                input_name = os.path.splitext(os.path.splitext(os.path.basename(file))[0])[0]
                input_files.append((os.path.join(input_file, file), os.path.join(output_file, input_name)))
    else:
        input_files.append((input_file, output_file))
    log.info(f"Processing {len(input_files)} files")
    for file_in, file_out in input_files:
        try:
            process_file(file_in, file_out, output_format, field1, values1, exact_match1, inverse1, field2, values2,
                         exact_match2, inverse2, from_date, to_date, single_field, csv_fields)
        except Exception as err:
            log.warning(f"Error processing {file_in}: {err}")
            log.warning(traceback.format_exc())