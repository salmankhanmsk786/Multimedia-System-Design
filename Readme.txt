Project 3: The Crucible
Overview: This project processes multimedia data, generates thumbnails from video files, and uploads them to Frame.io. The script integrates data from Baselight and Xytech files, processes video frames, creates thumbnails, and uploads them to a specified Frame.io project.

Prerequisites
Python 3.x
MongoDB installed and running locally.
FFmpeg installed and added to your system's PATH.
Frame.io API Token with appropriate permissions.

1. Installation
Clone the Repository:
git clone <repository-url>
cd The_Crucible

2. Install Required Packages:
Install Required Packages:

pip install -r requirements.txt

3.Install Frame.io Python Client:
pip install frameioclient

Usage
Command-Line Arguments
-b or --baselight: Path to the Baselight export file.
-x or --xytech: Path to the Xytech file.
-v or --video: Path to the video file.
-c or --command: Command to execute (process, video, export).
-t or --token: Frame.io API Token.
-p or --parent_id: Parent Asset ID for the Frame.io project.

Example Commands

1. Process Baselight and Xytech Data:
python The_Crucible.py -b Baselight_export.txt -x Xytech.txt -c process

2. Process Video and Generate Thumbnails:
python The_Crucible.py -v twitch_nft_demo.mp4 -c video

3. Export Data and Upload Thumbnails to Frame.io:
python The_Crucible.py -c export -b Baselight_export.txt -x Xytech.txt -v twitch_nft_demo.mp4


Script Details

Main Functions
* read_and_insert_baselight_data(filepath): Reads Baselight data and inserts it into MongoDB.
* read_and_insert_xytech_data(filepath): Reads Xytech data and inserts it into MongoDB.
* fetch_and_filter_frame_data(db, video_path, fps): Fetches and filters frame data based on video length.
* create_thumbnail(video_path, frame, fps): Creates a thumbnail for a given frame.
* upload_to_frameio(api_token, parent_asset_id, file_path): Uploads a file to Frame.io.
Running the Script
* Ensure MongoDB is running locally.
* Execute the script with the desired command-line arguments.

