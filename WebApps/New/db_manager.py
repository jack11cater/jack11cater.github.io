import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import sqlite3
import os
import sys
import subprocess
import threading
import time
from datetime import datetime

# --- Configuration ---
# Default names for your scripts and database
DB_FILENAME = 'geocoded_providers.sqlite'
SCRIPT_PARSE = 'provider_parse.py'
SCRIPT_UPDATE = 'update_provider_db.py'
SCRIPT_SERVER = 'back_end.py'
INPUT_CSV_DIR = '.'  # Directory to scan for geocoded CSVs

class DBManagerApp(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Provider Database Manager")
        self.geometry("900x700")
        
        # Set a reasonable min size
        self.minsize(800, 600)

        # Style configuration
        style = ttk.Style(self)
        style.theme_use('clam') # 'clam', 'alt', 'default', 'classic'
        
        # --- Variables ---
        self.db_path_var = tk.StringVar(value=os.path.abspath(DB_FILENAME))
        self.status_var = tk.StringVar(value="Idle")
        self.process = None # Handle for running subprocesses

        # --- Layout ---
        self.create_widgets()
        
        # Initial stats load
        self.refresh_db_stats()

    def create_widgets(self):
        # 1. Top Panel: Database Info
        info_frame = ttk.LabelFrame(self, text="Database Information", padding=10)
        info_frame.pack(fill=tk.X, padx=10, pady=5)

        # Row 1: Path & Refresh
        path_frame = ttk.Frame(info_frame)
        path_frame.pack(fill=tk.X, pady=2)
        ttk.Label(path_frame, text="DB Path:").pack(side=tk.LEFT)
        ttk.Entry(path_frame, textvariable=self.db_path_var, width=60, state='readonly').pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        ttk.Button(path_frame, text="Browse...", command=self.browse_db).pack(side=tk.LEFT)
        ttk.Button(path_frame, text="Refresh Stats", command=self.refresh_db_stats).pack(side=tk.LEFT, padx=5)

        # Row 2: Stats Grid
        stats_grid = ttk.Frame(info_frame)
        stats_grid.pack(fill=tk.X, pady=10)
        
        # Stat Labels
        self.lbl_row_count = ttk.Label(stats_grid, text="Total Providers: --", font=('Helvetica', 12, 'bold'))
        self.lbl_row_count.grid(row=0, column=0, padx=20, sticky="w")
        
        self.lbl_size = ttk.Label(stats_grid, text="File Size: --")
        self.lbl_size.grid(row=0, column=1, padx=20, sticky="w")
        
        self.lbl_modified = ttk.Label(stats_grid, text="Last Modified: --")
        self.lbl_modified.grid(row=0, column=2, padx=20, sticky="w")

        # 2. Middle Panel: Actions & Main Content
        middle_frame = ttk.Frame(self)
        middle_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Sidebar: Action Buttons
        actions_frame = ttk.LabelFrame(middle_frame, text="Actions", padding=10)
        actions_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 5))

        ttk.Button(actions_frame, text="1. Parse NPI Data", command=lambda: self.run_script("Parse Data", [sys.executable, SCRIPT_PARSE])).pack(fill=tk.X, pady=5)
        
        # Separator
        ttk.Separator(actions_frame, orient='horizontal').pack(fill=tk.X, pady=10)
        
        ttk.Label(actions_frame, text="Maintenance:").pack(anchor="w")
        ttk.Button(actions_frame, text="Update DB from CSVs", command=lambda: self.run_script("Update DB", [sys.executable, SCRIPT_UPDATE, self.db_path_var.get(), INPUT_CSV_DIR])).pack(fill=tk.X, pady=5)
        
        # Separator
        ttk.Separator(actions_frame, orient='horizontal').pack(fill=tk.X, pady=10)
        
        ttk.Label(actions_frame, text="Server:").pack(anchor="w")
        self.btn_server = ttk.Button(actions_frame, text="Start API Server", command=self.toggle_server)
        self.btn_server.pack(fill=tk.X, pady=5)
        
        # Stop Button (Global)
        self.btn_stop = ttk.Button(actions_frame, text="Stop Process", command=self.stop_process, state=tk.DISABLED)
        self.btn_stop.pack(fill=tk.X, pady=20)

        # Right Side: Console Output
        console_frame = ttk.LabelFrame(middle_frame, text="Console Output", padding=10)
        console_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.console_text = tk.Text(console_frame, bg="black", fg="lightgreen", insertbackground="white", font=("Consolas", 10))
        self.console_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(console_frame, orient="vertical", command=self.console_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.console_text['yscrollcommand'] = scrollbar.set

        # 3. Bottom Status Bar
        status_bar = ttk.Frame(self, relief=tk.SUNKEN)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        ttk.Label(status_bar, textvariable=self.status_var).pack(side=tk.LEFT, padx=5)

    # --- Database Logic ---
    def browse_db(self):
        filename = filedialog.askopenfilename(filetypes=[("SQLite Files", "*.sqlite;*.db")])
        if filename:
            self.db_path_var.set(filename)
            self.refresh_db_stats()

    def refresh_db_stats(self):
        db_path = self.db_path_var.get()
        if not os.path.exists(db_path):
            self.lbl_row_count.config(text="Total Providers: File not found")
            self.lbl_size.config(text="File Size: --")
            self.lbl_modified.config(text="Last Modified: --")
            return

        # Get file stats
        stats = os.stat(db_path)
        size_mb = stats.st_size / (1024 * 1024)
        mod_time = datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        
        self.lbl_size.config(text=f"File Size: {size_mb:.2f} MB")
        self.lbl_modified.config(text=f"Last Modified: {mod_time}")

        # Get DB Row Count
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM providers")
            count = cursor.fetchone()[0]
            self.lbl_row_count.config(text=f"Total Providers: {count:,}")
            conn.close()
        except sqlite3.Error as e:
            self.lbl_row_count.config(text=f"Total Providers: Error reading DB")
            self.log(f"Error reading database stats: {e}")

    # --- Script Execution Logic ---
    def log(self, message):
        self.console_text.insert(tk.END, message + "\n")
        self.console_text.see(tk.END)

    def run_script(self, action_name, command_list):
        if self.process:
            messagebox.showwarning("Busy", "A process is already running. Please wait or stop it first.")
            return

        self.status_var.set(f"Running: {action_name}...")
        self.btn_stop.config(state=tk.NORMAL)

        # Ask before clearing console so prior output (e.g. errors) isn't silently lost
        if self.console_text.get("1.0", tk.END).strip():
            if not messagebox.askyesno("Clear Console", "Clear the console output before starting?"):
                self.log(f"\n{'='*40}")  # Visual separator if user keeps old output
        else:
            self.console_text.delete(1.0, tk.END)

        self.log(f"--- Starting {action_name} ---")
        self.log(f"Command: {' '.join(command_list)}\n")

        # Run in a separate thread to keep GUI responsive
        threading.Thread(target=self._execute_subprocess, args=(command_list,), daemon=True).start()

    def _execute_subprocess(self, command_list):
        try:
            # Popen allows us to capture output in real-time
            self.process = subprocess.Popen(
                command_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            # Read stdout line by line
            for line in self.process.stdout:
                self.console_text.insert(tk.END, line)
                self.console_text.see(tk.END)
            
            self.process.wait()
            return_code = self.process.returncode
            
            if return_code == 0:
                self.log(f"\n--- Process finished successfully ---")
            else:
                self.log(f"\n--- Process failed with exit code {return_code} ---")

        except FileNotFoundError:
            self.log(f"\nError: Could not find script or executable: {command_list[0]}")
        except Exception as e:
            self.log(f"\nError executing process: {e}")
        finally:
            self.process = None
            self.btn_stop.config(state=tk.DISABLED)
            self.status_var.set("Idle")
            # If it wasn't the server running, refresh stats
            if SCRIPT_SERVER not in command_list[1]:
                self.after(100, self.refresh_db_stats) # update UI safely from thread

    def toggle_server(self):
        if self.process:
            # If running, stop it
            self.stop_process()
            self.btn_server.config(text="Start API Server")
        else:
            # Start server
            self.btn_server.config(text="Stop API Server")
            self.run_script("API Server", [sys.executable, SCRIPT_SERVER])

    def stop_process(self):
        if self.process:
            self.log("\n--- Stopping process... ---")
            self.process.terminate() # Try graceful termination
            # Give it a moment, then kill if necessary
            def kill_if_alive():
                if self.process and self.process.poll() is None:
                    self.process.kill()
                    self.log("--- Process killed forceably ---")
            
            self.after(2000, kill_if_alive)
            self.btn_server.config(text="Start API Server") # Reset server button text just in case

if __name__ == "__main__":
    app = DBManagerApp()
    app.mainloop()
