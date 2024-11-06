import csv
import sys
import time
import os
import pandas as pd
from thefuzz import fuzz
from tqdm import tqdm

def sort_large_csv_pandas(input_file, output_file, chunksize=100000):
    """
    Sorts a large CSV file using Pandas.

    Args:
        input_file: Path to the input CSV file.
        output_file: Path to the output CSV file.
        chunksize: Number of rows to process in each chunk.
    """

    df_list = []
    for chunk in pd.read_csv(input_file, chunksize=chunksize):
        df_list.append(chunk.sort_values(by=['message']))  # Replace 'column_to_sort' with the actual column name

    df = pd.concat(df_list)
    df.sort_values(by=['message'], inplace=True)
    df.to_csv(output_file, index=False)

def convert_to_csv(similarity_data_list, output_file):
    with open(output_file, 'w', newline='') as csvfile:
        # fieldnames = ['Matcher', 'Matches', 'Scores', 'Count']
        fieldnames = ['Matcher', 'Count']
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)

        for data in similarity_data_list:
            # writer.writerow([data.matcher, data.matches, data.scores, data.count])
            writer.writerow([data.matcher, data.count])
            # for match, score in zip(data.matches, data.scores):
                # writer.writerow([data.matcher, match, score, data.count])

class SimilarityData:
    def __init__(self, matcher: str, matches: list, scores: list, count: int):
        self.matcher = matcher      # String
        self.matches = matches      # List of strings
        self.scores = scores        # List of integers
        self.count = count          # Integer

    def update_matches_and_scores(self, new_match, new_score):
        if not self.matches:
            # Initialize an empty list if not already present
            self.matches = []
            self.scores = []
        self.matches.append(new_match)
        self.scores.append(new_score)
        self.count += 1

    def __str__(self):
        return f"Matcher: {self.matcher}, Matches: {self.matches}, Scores: {self.scores}, Count: {self.count}"

# Process CSV in chunks
def process_large_csv(file_path, output_file):
    # Adjust based on your memory capacity
    chunk_size = 10
    sleep_time = 0
    required_score = 50
    similarity_data_list = []
    skipped = 0
    current_message = 0

    for chunk in tqdm(pd.read_csv(file_path, chunksize=chunk_size), total=total_messages / chunk_size):
    # for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        messages = chunk['message'].tolist()
        for message in messages:
            current_message += 1
            if len(message) > 10000:
                skipped += 1
                continue
            # print(message)
            # print(len(message))
            time.sleep(sleep_time)

            # Now, find the similarity score of this message with existing messages
            matcher_found = False
            for index, data in enumerate(similarity_data_list):
                score = fuzz.ratio(data.matcher, message)

                if score >= required_score:
                    similarity_data_list[index].update_matches_and_scores(message, score)
                    matcher_found = True
                    break

            if not matcher_found:
                similarity_data_list.append(SimilarityData(message, [], [], 0))

        # print(current_message)
        if current_message >= total_messages:
            break

    print("Skipped: " + str(skipped))
    # Concatenate all results into a single DataFrame
    similarity_data_list.sort(key=lambda x: x.count, reverse=True)
    convert_to_csv(similarity_data_list, output_file)

# Specify file paths
base_path = '/Users/vaibhav.malik/Downloads/'
input_file_name = 'All-Messages-search-result.csv'
input_file_path = base_path + input_file_name
total_messages = 10000

if len(sys.argv) == 1:
    print("Input filename was not specified, defaulting to " + input_file_path)
    print("Total messages to scan were not specified, defaulting to " + str(total_messages) + "messages")
else:
    input_file_path = sys.argv[1]
    input_file_name = os.path.basename(input_file_path)
    total_messages = int(sys.argv[2])
    print("Input file: " + input_file_path)
    print("Total messages to be scanned: " + str(total_messages))

# Process the large CSV file
# sorted_input_file_path = base_path + "Sorted-" + input_file_name
# sort_large_csv_pandas(input_file_path, sorted_input_file_path)

output_file_path = base_path + "Analysis-" + input_file_name
process_large_csv(input_file_path, output_file_path)
# process_large_csv(sorted_input_file_path, output_file_path)
print("Similarity data saved to: " + output_file_path)
