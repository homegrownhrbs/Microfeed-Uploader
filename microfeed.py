import os
import json
import requests
import logging
import threading
import queue
import shutil
import time
import mimetypes
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import webbrowser
import subprocess

# Configure logging (we'll override handlers later to redirect to GUI)
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[]
)

def load_config(config_path='api.json'):
    """
    Loads configuration from a JSON file.
    Returns a dictionary with configuration parameters.
    """
    logging.info(f"Loading configuration from {config_path}...")
    if not Path(config_path).is_file():
        logging.error(f"Configuration file '{config_path}' not found. Please create it with the required parameters.")
        sys.exit(1)
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate required fields
        if 'api_key' not in config:
            logging.error("Configuration file is missing the 'api_key' field.")
            sys.exit(1)
        if 'microfeed_url' not in config:
            logging.error("Configuration file is missing the 'microfeed_url' field.")
            sys.exit(1)
        
        # Sanitize microfeed_url to remove trailing slash
        config['microfeed_url'] = config['microfeed_url'].rstrip('/')
        
        logging.info("Configuration loaded successfully.")
        return config
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing the configuration file: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error loading configuration: {e}")
        sys.exit(1)

def get_video_files(folder_path):
    """
    Retrieves a list of video files in the specified folder.
    Filters files based on common video file extensions.
    Returns a list of file paths.
    """
    logging.info("Scanning for video files in the selected folder...")
    video_extensions = ['.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.mpeg']
    video_files = [file for file in Path(folder_path).iterdir() if file.is_file() and file.suffix.lower() in video_extensions]
    
    if not video_files:
        logging.warning("No video files found in the selected folder.")
    else:
        logging.info(f"Found {len(video_files)} video file(s).")
    
    return video_files

def create_item(api_url, api_key, title, status="published"):
    """
    Creates a new item via the Microfeed API.
    Returns the item_id if successful, else None.
    """
    endpoint = f"{api_url}/api/items/"
    headers = {
        "Content-Type": "application/json",
        "X-MicrofeedAPI-Key": api_key
    }
    payload = {
        "title": title,
        "status": status
    }
    
    logging.info(f"Creating new item with title: '{title}' and status: '{status}'")
    try:
        response = requests.post(endpoint, headers=headers, json=payload)
        if response.status_code == 201:
            data = response.json()
            logging.debug(f"Create Item Response Data: {data}")  # Detailed debug info
            item_id = data.get("id")  # Assuming the response contains the new item's ID under 'id'
            if not item_id:
                logging.error(f"Failed to retrieve item_id from response: {data}")
                return None
            logging.info(f"Item created successfully with item_id: {item_id}")
            return item_id
        else:
            logging.error(f"Failed to create item. Status Code: {response.status_code}, Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to create item '{title}': {e}")
        return None

def generate_presigned_url(api_url, api_key, item_id, category, file_path):
    """
    Generates a presigned URL for uploading a media file to R2.
    Returns the presigned_url and media_url.
    """
    endpoint = f"{api_url}/api/media_files/presigned_urls/"
    headers = {
        "Content-Type": "application/json",
        "X-MicrofeedAPI-Key": api_key
    }
    payload = {
        "item_id": item_id,
        "category": category,
        "full_local_file_path": str(file_path)
    }
    
    logging.info(f"Requesting presigned URL for file: {file_path.name}")
    try:
        response = requests.post(endpoint, headers=headers, json=payload)
        if response.status_code == 201:
            data = response.json()
            logging.debug(f"Presigned URL Response Data: {data}")  # Detailed debug info
            presigned_url = data.get("presigned_url")
            media_url = data.get("media_url")
            if not presigned_url or not media_url:
                logging.error(f"Invalid response for file {file_path.name}: {data}")
                return None, None
            logging.info(f"Received presigned URL for {file_path.name}")
            return presigned_url, media_url
        else:
            logging.error(f"Failed to get presigned URL. Status Code: {response.status_code}, Response: {response.text}")
            return None, None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get presigned URL for {file_path.name}: {e}")
        return None, None

class ProgressFile:
    """
    A file-like object that wraps another file object and tracks the number of bytes read.
    """

    def __init__(self, file, progress_callback):
        self.file = file
        self.progress_callback = progress_callback
        self.total = os.path.getsize(file.name)
        self.read_bytes = 0

    def read(self, chunk_size):
        data = self.file.read(chunk_size)
        if data:
            self.read_bytes += len(data)
            self.progress_callback(self.read_bytes, self.total)
        return data

    def __getattr__(self, attr):
        return getattr(self.file, attr)

def upload_file(presigned_url, file_path, progress_callback=None, max_retries=3, backoff_factor=0.3):
    """
    Uploads the file to R2 using the provided presigned URL.
    Returns True if upload is successful, False otherwise.
    """
    logging.info(f"Uploading {file_path.name} to R2...")

    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["PUT"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        with open(file_path, 'rb') as f:
            if progress_callback:
                f = ProgressFile(f, progress_callback)
            response = session.put(presigned_url, data=f, timeout=300)  # Increased timeout for large files
            if response.status_code in [200, 201, 204]:
                logging.info(f"Successfully uploaded {file_path.name} to R2.")
                return True
            else:
                logging.error(f"Failed to upload {file_path.name}. Status Code: {response.status_code}, Response: {response.text}")
                return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to upload {file_path.name}: {e}")
        return False
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return False

def fetch_item(api_url, api_key, item_id):
    """
    Fetches item details via the Admin API.
    Returns the item data if successful, else None.
    """
    endpoint = f"{api_url}/api/items/{item_id}/"
    headers = {
        "Content-Type": "application/json",
        "X-MicrofeedAPI-Key": api_key
    }
    
    logging.info(f"Fetching item details for item_id: {item_id}")
    try:
        response = requests.get(endpoint, headers=headers, timeout=30)  # Added timeout
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Item {item_id} details fetched successfully.")
            return data
        else:
            logging.error(f"Failed to fetch item {item_id}. Status Code: {response.status_code}, Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch item {item_id}: {e}")
        return None

def update_item_with_attachment(api_url, api_key, item_id, media_url, mime_type, size_in_bytes, title, status):
    """
    Updates the created item with the attachment's media_url.
    Returns True if update is successful, False otherwise.
    """
    endpoint = f"{api_url}/api/items/{item_id}/"
    headers = {
        "Content-Type": "application/json",
        "X-MicrofeedAPI-Key": api_key
    }
    payload = {
        "title": title,
        "status": status,
        "attachment": {  # Corrected key and structure
            "category": "video",
            "url": media_url,
            "mime_type": mime_type,
            "size_in_bytes": size_in_bytes
        }
    }
    
    logging.info(f"Updating item {item_id} with attachment URL.")
    try:
        response = requests.put(endpoint, headers=headers, json=payload)
        logging.debug(f"Update Item Response Status: {response.status_code}")
        logging.debug(f"Update Item Response Body: {response.text}")
        if response.status_code == 200:
            logging.info(f"Item {item_id} updated successfully with attachment.")
            return True
        elif response.status_code == 404:
            logging.error(f"Item {item_id} not found. Status Code: {response.status_code}, Response: {response.text}")
            return False
        else:
            logging.error(f"Failed to update item {item_id}. Status Code: {response.status_code}, Response: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to update item {item_id} with attachment: {e}")
        return False

def process_files(root):
    # Load configuration from api.json
    config = load_config()
    api_key = config['api_key']
    microfeed_url = config['microfeed_url']

    # Get selected folder path
    folder_path = root.selected_folder

    # Update status
    root.queue.put({'status': f"Scanning folder {folder_path} for video files..."})

    # Get list of video files
    video_files = get_video_files(folder_path)
    if not video_files:
        root.queue.put({'status': "No video files to upload. Exiting."})
        root.queue.put({'enable_buttons': True})
        return

    total_files = len(video_files)
    files_processed = 0

    # Initialize per-file progress variables
    for file_path in video_files:
        root.file_info[file_path.name] = {
            'progress': tk.DoubleVar(),
            'status': tk.StringVar(value="Pending"),
            'speed': tk.StringVar(value="0 KB/s"),
            'file_location': str(file_path),
            'item_id': '',
            'api_link': '',
            'file_name': file_path.name
        }
        # Add to the Treeview
        root.treeview.insert('', 'end', iid=file_path.name, values=(
            file_path.name,  # File Name
            "Pending",       # Status
            "0.00",          # Progress
            "0 KB/s",        # Speed
            str(file_path),  # File Location
            "",              # Item ID
            ""               # API Link
        ))
        root.start_times[file_path.name] = time.time()

    for idx, file_path in enumerate(video_files):
        root.queue.put({'status': f"Processing file {file_path.name} ({idx+1}/{total_files})"})
        # Update file status
        root.queue.put({'file_status': (file_path.name, "Processing")})

        # Check file size
        size_in_bytes = file_path.stat().st_size
        if size_in_bytes > 4.8 * 1024 * 1024 * 1024:
            # Move file to 'too_large' folder
            too_large_folder = Path(folder_path) / "too_large"
            too_large_folder.mkdir(exist_ok=True)
            destination = too_large_folder / file_path.name
            try:
                shutil.move(str(file_path), str(destination))
                logging.info(f"Moved {file_path.name} to 'too_large' folder.")
                root.queue.put({'status': f"File {file_path.name} is too large (>4.8GB). Moved to 'too_large' folder."})
                root.queue.put({'file_status': (file_path.name, "Too Large")})
                # Update file location
                root.queue.put({'file_location': (file_path.name, str(destination))})
            except Exception as e:
                logging.error(f"Failed to move {file_path.name} to 'too_large' folder: {e}")
                root.queue.put({'status': f"Failed to move {file_path.name} to 'too_large' folder: {e}"})
                root.queue.put({'file_status': (file_path.name, "Error")})
            continue

        # Extract title from file name (without extension)
        title = file_path.stem
        status = "published"  # Can be modified or made dynamic if needed

        # Step 1: Create a new item
        item_id = create_item(
            api_url=microfeed_url,
            api_key=api_key,
            title=title,
            status=status
        )

        if not item_id:
            logging.error(f"Skipping file {file_path.name} due to item creation failure.")
            root.queue.put({'status': f"Skipping file {file_path.name} due to item creation failure."})
            root.queue.put({'file_status': (file_path.name, "Error")})
            continue

        # Update item ID in GUI
        root.queue.put({'file_item_id': (file_path.name, item_id)})

        # Step 2: Generate presigned URL
        presigned_url, media_url = generate_presigned_url(
            api_url=microfeed_url,
            api_key=api_key,
            item_id=item_id,
            category="video",
            file_path=file_path
        )

        if not presigned_url or not media_url:
            logging.error(f"Skipping upload for {file_path.name} due to presigned URL failure.")
            root.queue.put({'status': f"Skipping upload for {file_path.name} due to presigned URL failure."})
            root.queue.put({'file_status': (file_path.name, "Error")})
            continue

        # Update API Link in GUI
        root.queue.put({'file_api_link': (file_path.name, media_url)})

        # Step 3: Upload the file to R2
        def progress_callback(bytes_uploaded, total_size):
            percentage = bytes_uploaded / total_size * 100
            elapsed_time = time.time() - root.start_times[file_path.name]
            speed = bytes_uploaded / elapsed_time if elapsed_time > 0 else 0
            speed_str = f"{speed / 1024:.2f} KB/s"
            root.queue.put({'file_progress': (file_path.name, percentage)})
            root.queue.put({'file_speed': (file_path.name, speed_str)})
            # Optionally update status message with speed, etc.

        # Record the start time for speed calculation
        root.start_times[file_path.name] = time.time()

        success = upload_file(presigned_url, file_path, progress_callback=progress_callback)
        if not success:
            logging.error(f"Failed to upload {file_path.name}.")
            root.queue.put({'status': f"Failed to upload {file_path.name}."})
            root.queue.put({'file_status': (file_path.name, "Upload Failed")})
            continue

        # Wait briefly to ensure the file is accessible
        time.sleep(5)

        # Step 4: Fetch item details
        item_data = fetch_item(
            api_url=microfeed_url,
            api_key=api_key,
            item_id=item_id
        )

        if not item_data:
            logging.error(f"Failed to fetch item details for {item_id}. Skipping update.")
            root.queue.put({'status': f"Failed to fetch item details for {item_id}. Skipping update."})
            root.queue.put({'file_status': (file_path.name, "Error")})
            continue

        # Step 5: Determine MIME type and size_in_bytes
        mime_type, _ = mimetypes.guess_type(file_path.name)
        if mime_type is None:
            mime_type = 'application/octet-stream'
            logging.warning(f"Could not determine MIME type for {file_path.name}. Using default '{mime_type}'.")

        size_in_bytes = file_path.stat().st_size

        # Step 6: Update the item with attachment details
        update_success = update_item_with_attachment(
            api_url=microfeed_url,
            api_key=api_key,
            item_id=item_id,
            media_url=media_url,
            mime_type=mime_type,
            size_in_bytes=size_in_bytes,
            title=title,
            status=status
        )

        if update_success:
            logging.info(f"Media URL for {file_path.name}: {media_url}")
            root.queue.put({'status': f"Successfully processed {file_path.name}."})
            root.queue.put({'file_status': (file_path.name, "Completed")})
            # Move file to 'processed' folder
            processed_folder = Path(folder_path) / "processed"
            processed_folder.mkdir(exist_ok=True)
            destination = processed_folder / file_path.name
            try:
                shutil.move(str(file_path), str(destination))
                logging.info(f"Moved {file_path.name} to 'processed' folder.")
                root.queue.put({'status': f"Moved {file_path.name} to 'processed' folder."})
                # Update file location in GUI
                root.queue.put({'file_location': (file_path.name, str(destination))})
            except Exception as e:
                logging.error(f"Failed to move {file_path.name} to 'processed' folder: {e}")
                root.queue.put({'status': f"Failed to move {file_path.name} to 'processed' folder: {e}"})
        else:
            logging.error(f"Failed to update item {item_id} with attachment for {file_path.name}.")
            root.queue.put({'status': f"Failed to update item {item_id} with attachment for {file_path.name}."})
            root.queue.put({'file_status': (file_path.name, "Error")})

        # Update overall progress
        files_processed += 1
        overall_percentage = files_processed / total_files * 100
        root.queue.put({'overall_progress': overall_percentage})

        # Ensure final progress is set to 100%
        root.queue.put({'file_progress': (file_path.name, 100)})

    root.queue.put({'status': "All files processed."})
    root.queue.put({'enable_buttons': True})

def main():
    # Initialize Tkinter main window
    root = tk.Tk()
    root.title("Video Uploader")
    root.geometry("1200x1200")  # Expanded window size (adjusted to fit most screens)

    # Variables to hold folder path and status messages
    folder_path_var = tk.StringVar()
    folder_path_var.set("No folder selected.")
    
    status_var = tk.StringVar()
    status_var.set("Status: Waiting to start.")

    # Create frames for better layout management
    top_frame = tk.Frame(root)
    top_frame.pack(fill='x', padx=10, pady=5)

    middle_frame = tk.Frame(root)
    middle_frame.pack(fill='both', expand=True, padx=10, pady=5)

    bottom_frame = tk.Frame(root)
    bottom_frame.pack(fill='both', expand=True, padx=10, pady=5)

    # Labels and Buttons
    folder_label = tk.Label(top_frame, textvariable=folder_path_var)
    folder_label.pack(side='left', padx=5)

    select_folder_button = tk.Button(top_frame, text="Select Folder", command=lambda: select_folder_action(root, folder_path_var))
    select_folder_button.pack(side='left', padx=5)

    start_button = tk.Button(top_frame, text="Start Upload", command=lambda: start_upload_action(root))
    start_button.pack(side='left', padx=5)

    # Overall progress bar
    overall_progress_label = tk.Label(top_frame, text="Overall Progress:")
    overall_progress_label.pack(side='left', padx=5)

    overall_progress_var = tk.DoubleVar()
    overall_progress_bar = ttk.Progressbar(top_frame, variable=overall_progress_var, maximum=100, length=200)
    overall_progress_bar.pack(side='left', padx=5)

    # File status Treeview
    columns = ("File Name", "Status", "Progress", "Speed", "File Location", "Item ID", "API Link")
    treeview = ttk.Treeview(middle_frame, columns=columns, show='headings')
    treeview.heading("File Name", text="File Name")
    treeview.heading("Status", text="Status")
    treeview.heading("Progress", text="Progress (%)")
    treeview.heading("Speed", text="Speed")
    treeview.heading("File Location", text="File Location")
    treeview.heading("Item ID", text="Item ID")
    treeview.heading("API Link", text="API Link")
    treeview.column("File Name", width=200)
    treeview.column("Status", width=100)
    treeview.column("Progress", width=100, anchor='center')
    treeview.column("Speed", width=100, anchor='center')
    treeview.column("File Location", width=300)
    treeview.column("Item ID", width=100)
    treeview.column("API Link", width=300)
    treeview.pack(fill='both', expand=True, side='left')

    # Scrollbar for the Treeview
    treeview_scrollbar = ttk.Scrollbar(middle_frame, orient="vertical", command=treeview.yview)
    treeview.configure(yscrollcommand=treeview_scrollbar.set)
    treeview_scrollbar.pack(side='right', fill='y')

    # Make API Link and File Location columns clickable
    def on_treeview_click(event):
        item_id = treeview.identify_row(event.y)
        column = treeview.identify_column(event.x)
        if not item_id or not column:
            return
        column_index = int(column.replace('#', '')) - 1
        columns = treeview['columns']
        clicked_column = columns[column_index]
        if clicked_column == "API Link":
            api_link = treeview.item(item_id, 'values')[columns.index("API Link")]
            if api_link:
                # Confirm before opening
                if messagebox.askyesno("Open Link", f"Do you want to open this link?\n{api_link}"):
                    webbrowser.open(api_link)
        elif clicked_column == "File Location":
            file_location = treeview.item(item_id, 'values')[columns.index("File Location")]
            if file_location:
                # Confirm before opening
                if messagebox.askyesno("Open File Location", f"Do you want to open the file location?\n{file_location}"):
                    open_file_location(file_location)

    treeview.bind("<ButtonRelease-1>", on_treeview_click)

    # Change cursor to hand when hovering over clickable cells
    def on_treeview_motion(event):
        item_id = treeview.identify_row(event.y)
        column = treeview.identify_column(event.x)
        if not item_id or not column:
            treeview.config(cursor="")
            return
        column_index = int(column.replace('#', '')) - 1
        columns = treeview['columns']
        clicked_column = columns[column_index]
        if clicked_column in ("API Link", "File Location"):
            treeview.config(cursor="hand2")
        else:
            treeview.config(cursor="")

    treeview.bind("<Motion>", on_treeview_motion)

    def open_file_location(path):
        if sys.platform == 'win32':
            # For Windows
            subprocess.Popen(f'explorer /select,"{path}"')
        elif sys.platform == 'darwin':
            # For macOS
            subprocess.Popen(['open', '-R', path])
        else:
            # For Linux
            subprocess.Popen(['xdg-open', os.path.dirname(path)])

    # Console log Text widget (make it bigger)
    console_label = tk.Label(bottom_frame, text="Console Log:")
    console_label.pack(anchor='w')

    console_text = tk.Text(bottom_frame, wrap='word', height=15)  # Increased height
    console_text.pack(fill='both', expand=True)

    console_scrollbar = ttk.Scrollbar(console_text, orient='vertical', command=console_text.yview)
    console_text['yscrollcommand'] = console_scrollbar.set
    console_scrollbar.pack(side='right', fill='y')

    # Redirect logging to console_text widget
    class TextHandler(logging.Handler):
        def __init__(self, text_widget):
            logging.Handler.__init__(self)
            self.text_widget = text_widget

        def emit(self, record):
            msg = self.format(record)
            def append():
                self.text_widget.insert(tk.END, msg + '\n')
                self.text_widget.see(tk.END)
            self.text_widget.after(0, append)

    logger = logging.getLogger()
    text_handler = TextHandler(console_text)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    text_handler.setFormatter(formatter)
    logger.addHandler(text_handler)

    # Create a queue to communicate with the GUI thread
    root.queue = queue.Queue()
    root.selected_folder = None
    root.file_info = {}
    root.treeview = treeview
    root.start_times = {}

    # Variables to track per-file progress and status
    def select_folder_action(root, folder_path_var):
        folder_selected = filedialog.askdirectory(title="Select Folder Containing Videos")
        if folder_selected:
            folder_path_var.set(f"Selected folder: {folder_selected}")
            # Save selected folder path to a variable accessible to processing thread
            root.selected_folder = folder_selected

    def start_upload_action(root):
        # Start the processing in a new thread
        if not hasattr(root, 'selected_folder') or not root.selected_folder:
            messagebox.showerror("Error", "Please select a folder first.")
            return
        # Disable the buttons to prevent multiple starts
        select_folder_button.config(state='disabled')
        start_button.config(state='disabled')
        # Initialize dictionaries to track per-file info
        root.start_times = {}
        root.file_info = {}
        # Start the processing thread
        processing_thread = threading.Thread(target=process_files, args=(root,))
        processing_thread.start()
        # Start the queue checking
        root.after(100, check_queue)

    def check_queue():
        try:
            while True:
                message = root.queue.get_nowait()
                if 'status' in message:
                    status_var.set(message['status'])
                if 'overall_progress' in message:
                    overall_progress_var.set(message['overall_progress'])
                if 'file_progress' in message:
                    file_name, percentage = message['file_progress']
                    # Update the progress in the Treeview
                    root.treeview.set(file_name, "Progress", f"{percentage:.2f}")
                if 'file_status' in message:
                    file_name, status = message['file_status']
                    root.treeview.set(file_name, "Status", status)
                if 'file_speed' in message:
                    file_name, speed = message['file_speed']
                    root.treeview.set(file_name, "Speed", speed)
                if 'file_item_id' in message:
                    file_name, item_id = message['file_item_id']
                    root.treeview.set(file_name, "Item ID", item_id)
                if 'file_api_link' in message:
                    file_name, api_link = message['file_api_link']
                    root.treeview.set(file_name, "API Link", api_link)
                if 'file_location' in message:
                    file_name, location = message['file_location']
                    root.treeview.set(file_name, "File Location", location)
                if 'enable_buttons' in message:
                    select_folder_button.config(state='normal')
                    start_button.config(state='normal')
        except queue.Empty:
            pass
        root.after(100, check_queue)

    # Status label
    status_label = tk.Label(bottom_frame, textvariable=status_var)
    status_label.pack(anchor='w')

    root.mainloop()

if __name__ == "__main__":
    main()
