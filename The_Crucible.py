# Project 3: The Crucible
# Salman Khan
# 05/03/2024
# Professor, Chaja

# How to use the script with command line arguments
# Note: before runnning the script delete thumbnails folder and output.csv file to create new thumbnails and csv file else it will append the data to the existing csv file.

# python The_Crucible.py -b Baselight_export.txt -x Xytech.txt -c process

# python The_Crucible.py -v twitch_nft_demo.mp4 -c video

# python The_Crucible.py -c export -b Baselight_export.txt -x Xytech.txt -v twitch_nft_demo.mp4

import pandas as pd
import subprocess
from pymongo import MongoClient
import argparse
import os
import time
from frameioclient import FrameioClient
from openpyxl import Workbook
from PIL import Image as PILImage
from openpyxl.drawing.image import Image as OpenpyxlImage
from urllib3.util import Retry

# Patch the Retry class
original_retry = Retry.__init__

def patched_retry(self, *args, **kwargs):
    if 'method_whitelist' in kwargs:
        kwargs['allowed_methods'] = kwargs.pop('method_whitelist')
    original_retry(self, *args, **kwargs)

Retry.__init__ = patched_retry


# Establish a connection to MongoDB
client = MongoClient('localhost', 27017)

# Create or connect to a database
db = client['TheCrucible']

# Create collections for Baselight and Xytech data
baselight_collection = db['baselight']
xytech_collection = db['xytech']

# Function to insert data into a collection
def insert_data(collection, data):
    collection.insert_one(data)

def read_and_insert_baselight_data(filepath_baselight):
    with open(filepath_baselight, 'r') as file:
        lines = file.readlines()
    for line in lines:
        if line.strip():
            parts = line.split()
            file_path = parts[0]
            frame_numbers = [part for part in parts[1:] if part.isdigit()]
            data = {
                "file_path": file_path,
                "frames": list(map(int, frame_numbers))
            }
            baselight_collection.insert_one(data)

def read_baselight_file(filepath_baselight):
    with open(filepath_baselight, 'r') as file:
        lines = file.readlines()
    baselight_data = []
    for line in lines:
        if line.strip():
            parts = line.split()
            file_path = parts[0]
            frame_numbers = [int(part) for part in parts[1:] if part.isdigit()]
            if frame_numbers:
                baselight_data.append((file_path, frame_numbers))
    return baselight_data

baselight_data = read_baselight_file('Baselight_export.txt')

def read_and_insert_xytech_data(filepath):
    with open(filepath, 'r') as file:
        lines = file.read().splitlines()
    xytech_data = {'producer': '', 'operator': '', 'job': '', 'notes': '', 'locations': []}
    current_section = None
    for line in lines:
        if 'Producer:' in line:
            xytech_data['producer'] = line.split(': ')[1].strip()
        elif 'Operator:' in line:
            xytech_data['operator'] = line.split(': ')[1].strip()
        elif 'Job:' in line:
            xytech_data['job'] = line.split(': ')[1].strip()
        elif 'Location' in line:
            current_section = 'locations'
        elif 'Notes:' in line:
            current_section = 'notes'
        elif current_section == 'locations' and line.strip() != '':
            xytech_data['locations'].append(line.strip())
        elif current_section == 'notes':
            xytech_data['notes'] += line.strip() + ' '
    xytech_collection.insert_one(xytech_data)

def read_xytech_file(filepath):
    with open(filepath, 'r') as file:
        lines = file.read().splitlines()
    xytech_data = {'producer': '', 'operator': '', 'job': '', 'notes': '', 'locations': []}
    current_section = None
    for line in lines:
        if 'Producer:' in line:
            xytech_data['producer'] = line.split(': ')[1].strip()
        elif 'Operator:' in line:
            xytech_data['operator'] = line.split(': ')[1].strip()
        elif 'Job:' in line:
            xytech_data['job'] = line.split(': ')[1].strip()
        elif 'Location' in line:
            current_section = 'locations'
        elif 'Notes:' in line:
            current_section = 'notes'
        elif current_section == 'locations' and line.strip() != '':
            xytech_data['locations'].append(line.strip())
        elif current_section == 'notes':
            xytech_data['notes'] += line.strip() + ' '
    return xytech_data

xytech_data = read_xytech_file('Xytech.txt')

path_mapping = {
    '/baselightfilesystem1/Dune2/reel1/partA/1920x1080': '/hpsans13/production/Dune2/reel1/partA/1920x1080',
    '/baselightfilesystem1/Dune2/reel1/VFX/Hydraulx': '/hpsans12/production/Dune2/reel1/VFX/Hydraulx',
    '/baselightfilesystem1/Dune2/pickups/shot_1ab/1920x1080': '/hpsans15/production/Dune2/pickups/shot_1ab/1920x1080',
    '/baselightfilesystem1/Dune2/reel1/VFX/Framestore': '/hpsans13/production/Dune2/reel1/VFX/Framestore',
    '/baselightfilesystem1/Dune2/reel1/VFX/AnimalLogic': '/hpsans14/production/Dune2/reel1/VFX/AnimalLogic',
    '/baselightfilesystem1/Dune2/reel1/partB/1920x1080': '/hpsans13/production/Dune2/reel1/partB/1920x1080',
    '/baselightfilesystem1/Dune2/pickups/shot_2b/1920x1080': '/hpsans11/production/Dune2/pickups/shot_2b/1920x1080',
}

baselight_data = [(path_mapping.get(file_path, file_path), frame_numbers) for file_path, frame_numbers in baselight_data]

def format_frame_ranges(frames):
    if not frames:
        return []
    frames.sort()
    ranges = []
    start = frames[0]
    end = start
    for frame in frames[1:]:
        if frame == end + 1:
            end = frame
        else:
            ranges.append((start, end))
            start = frame
            end = frame
    ranges.append((start, end))
    return ranges

def get_video_duration(video_path):
    command = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
    result = subprocess.run(command, stdout=subprocess.PIPE, text=True)
    return float(result.stdout.strip())

def fetch_and_filter_frame_data(db, video_path, fps=24):
    video_duration = get_video_duration(video_path)
    max_frames = video_duration * fps
    documents = db['baselight'].find()
    valid_frame_data = []
    for doc in documents:
        frames = [int(frame) for frame in doc['frames']]
        frame_ranges = calculate_frame_ranges(frames)
        for start_frame, end_frame in frame_ranges:
            if end_frame <= max_frames:
                valid_frame_data.append({
                    'file_path': doc['file_path'],
                    'frame_range': (start_frame, end_frame)
                })
    return valid_frame_data

def frame_to_timecode(frame, fps=24):
    total_seconds = frame / fps
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    frames = int((seconds - int(seconds)) * fps)
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}:{frames:02}"

def ensure_thumbnail_directory_exists():
    directory = "thumbnails"
    if not os.path.exists(directory):
        os.makedirs(directory)

def create_thumbnail(video_path, frame, fps):
    if not video_path:
        print("Error: video_path is not provided.")
        return None
    thumbnail_filename = f"thumbnail_{frame}_{int(time.time())}.jpg"
    thumbnail_path = os.path.join('thumbnails', thumbnail_filename)
    ensure_thumbnail_directory_exists()
    seconds = frame / fps
    command = [
        'ffmpeg', '-y', '-ss', str(seconds), '-i', video_path,
        '-frames:v', '1', '-q:v', '2', thumbnail_path
    ]
    try:
        subprocess.run(command, check=True)
        print(f"Thumbnail created and saved to {thumbnail_path}")

        # Resize to 96x74
        img = PILImage.open(thumbnail_path)
        img.thumbnail((96, 74))
        img.save(thumbnail_path)

        return thumbnail_path
    except subprocess.CalledProcessError as e:
        print(f"Failed to create thumbnail for frame {frame}: {e}")
        return None

def process_video_frames(filtered_frame_data, video_path, fps=24):
    video_duration = get_video_duration(video_path) * fps
    if video_path is not None:
        for data in filtered_frame_data:
            start_frame, end_frame = data['frame_range']
            if end_frame / fps <= video_duration:
                mid_frame = (start_frame + end_frame) // 2
                thumbnail_path = create_thumbnail(video_path, mid_frame, fps)
                if thumbnail_path:
                    print(f"Thumbnail for range {start_frame}-{end_frame} saved to {thumbnail_path}")
            else:
                print("video_path is None")

def calculate_frame_ranges(frames):
    if not frames:
        return []
    frames.sort()
    ranges = []
    start = frames[0]
    end = start
    for frame in frames[1:]:
        if frame == end + 1:
            end = frame
        else:
            ranges.append((start, end))
            start = frame
            end = frame
    ranges.append((start, end))
    return ranges

def merge_and_export_csv(baselight_data, xytech_data, video_path, csv_filepath, xls_filepath, fps=24):
    merged_data = []
    for file_path, frame_numbers in baselight_data:
        frame_ranges = format_frame_ranges(frame_numbers)
        for start_frame, end_frame in frame_ranges:
            if end_frame <= get_video_duration(video_path) * fps:
                thumbnail_path = create_thumbnail(video_path, (start_frame + end_frame) // 2, fps)
                timecode_start = frame_to_timecode(start_frame, fps)
                timecode_end = frame_to_timecode(end_frame, fps)
                entry = {
                    'Producer': xytech_data.get('producer', ''),
                    'Operator': xytech_data.get('operator', ''),
                    'Job': xytech_data.get('job', ''),
                    'Notes': xytech_data.get('notes', ''),
                    'Show Location': file_path,
                    'Frames to Fix': f"{start_frame}-{end_frame}",
                    'Timecode Range': f"{timecode_start} - {timecode_end}",
                    'Thumbnail': thumbnail_path
                }
                merged_data.append(entry)
                print(f"Processed: {entry}")
                upload_to_frameio(api_token, parent_asset_id, thumbnail_path)  # Add this line
    df = pd.DataFrame(merged_data)
    df.to_csv(csv_filepath, index=False)
    #df.to_excel(xls_filepath, index=False)

    # Export to XLSX with thumbnails
    wb = Workbook()
    ws = wb.active
    ws.append(['Producer', 'Operator', 'Job', 'Notes', 'Show Location', 'Frames to Fix', 'Timecode Range', 'Thumbnail'])
    for data in merged_data:
        img = OpenpyxlImage(data['Thumbnail'])
        ws.append([data['Producer'], data['Operator'], data['Job'], data['Notes'], data['Show Location'], data['Frames to Fix'], data['Timecode Range']])
        ws.add_image(img, f"H{ws.max_row}")
    wb.save(xls_filepath)
    print(f"Data exported successfully to {csv_filepath} and {xls_filepath}")


def upload_to_frameio(token, parent_asset_id, file_path):
    client = FrameioClient(token)
    try:
        client.assets.upload(parent_asset_id, file_path)
        print(f"Successfully uploaded {file_path} to Frame.io")
    except Exception as e:
        print(f"Failed to upload {file_path}: {e}")

api_token = 'fio-u-QnXfiuodqz1AHPcL61Lt3EUrJwLMSD9kUAkGjvjJNL712LSagNcndvHSvBV_WnX9'
parent_asset_id = '4f2e034c-5ed7-45a9-bafc-5786a6df6dd5'

def main():
    parser = argparse.ArgumentParser(description="Process and integrate multimedia data for Project 3.")
    parser.add_argument('-b', '--baselight', type=str, help="Path to the Baselight export file.")
    parser.add_argument('-x', '--xytech', type=str, help="Path to the Xytech file.")
    parser.add_argument('-v', '--video', type=str, help="Path to the video file.")
    parser.add_argument('-c', '--command', choices=['process', 'video', 'export'], help="Command to execute.")

    args = parser.parse_args()

    if args.command == 'process':
        print(f"Processing data with Baselight: {args.baselight} and Xytech: {args.xytech}")
        read_and_insert_baselight_data(args.baselight)
        read_and_insert_xytech_data(args.xytech)
        print("Data inserted successfully.")

    elif args.command == 'video':
        print("Handling video file:", args.video)
        filtered_frame_data = fetch_and_filter_frame_data(db, args.video)
        print("Filtered frame data based on video length:", filtered_frame_data)
        process_video_frames(filtered_frame_data, args.video)

    elif args.command == 'export':
        print("Exporting data to CSV and XLS")
        csv_filepath = "output.csv"
        xls_filepath = "output.xlsx"
        merge_and_export_csv(baselight_data, xytech_data, args.video, csv_filepath, xls_filepath)
        
        
if __name__ == "__main__":
    main()
