import os
import zipfile
import sqlite3
import webbrowser
from datetime import datetime, timedelta, timezone

import tkinter as tk
from tkinter import filedialog, ttk, messagebox
from tkinter.scrolledtext import ScrolledText

import pandas as pd
import folium
from folium.plugins import MarkerCluster, TimestampedGeoJson
import simplekml
from PIL import Image, ImageTk


# =========================================================
# CONSTANTS
# =========================================================

APPLE_EPOCH = datetime(2001, 1, 1, tzinfo=timezone.utc)
MPS_TO_MPH = 2.23694

IOS_CACHE_PATH = "private/var/mobile/Library/Caches/com.apple.routined/Cache.sqlite"
IOS_WAL_PATH = IOS_CACHE_PATH + "-wal"


# =========================================================
# CONVERSION HELPERS
# =========================================================

def apple_to_utc(ts):
    try:
        return (APPLE_EPOCH + timedelta(seconds=float(ts))).isoformat()
    except Exception:
        return ""


def mps_to_mph(speed):
    try:
        return round(float(speed) * MPS_TO_MPH, 4)
    except Exception:
        return ""


def convert_timezone(ts_utc_str, tz_str):
    if not ts_utc_str:
        return ""

    try:
        ts_utc = datetime.fromisoformat(ts_utc_str)

        if tz_str == "UTC":
            return ts_utc.isoformat()

        offset_hours = int(tz_str.replace("UTC", ""))
        tz = timezone(timedelta(hours=offset_hours))
        return ts_utc.astimezone(tz).isoformat()
    except Exception:
        return ""


def get_fade_duration_iso(fade_value):
    mapping = {
        "None": None,
        "5 minutes": "PT5M",
        "15 minutes": "PT15M",
        "30 minutes": "PT30M",
        "1 hour": "PT1H",
        "6 hours": "PT6H",
        "12 hours": "PT12H",
        "1 day": "P1D",
    }
    return mapping.get(fade_value, None)


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


# =========================================================
# DATAFRAME HELPERS
# =========================================================

def normalize_columns(df):
    rename_map = {}

    ts_local_candidates = ["Timestamp_Local", "timestamp_local"]
    ts_utc_candidates = ["TimestampUTC", "timestamp_utc", "timestamputc"]
    lat_candidates = ["Latitude", "latitude", "lat", "LAT", "centerLat"]
    lon_candidates = ["Longitude", "longitude", "lon", "LON", "centerLng", "lng"]
    acc_candidates = ["HorizontalAccuracy", "horizontalaccuracy", "accuracy", "Accuracy"]

    for c in ts_local_candidates:
        if c in df.columns:
            rename_map[c] = "Timestamp_Local"
            break

    for c in ts_utc_candidates:
        if c in df.columns:
            rename_map[c] = "TimestampUTC"
            break

    for c in lat_candidates:
        if c in df.columns:
            rename_map[c] = "Latitude"
            break

    for c in lon_candidates:
        if c in df.columns:
            rename_map[c] = "Longitude"
            break

    for c in acc_candidates:
        if c in df.columns:
            rename_map[c] = "HorizontalAccuracy"
            break

    return df.rename(columns=rename_map)


def apply_datetime_filter(df, start_str="", end_str=""):
    """
    Filters dataframe using Timestamp_Local first, then TimestampUTC.
    Format must be YYYY-MM-DD HH:MM
    Handles timezone-aware and timezone-naive timestamps safely.
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df = normalize_columns(df)

    ts_col = None
    if "Timestamp_Local" in df.columns:
        ts_col = "Timestamp_Local"
    elif "TimestampUTC" in df.columns:
        ts_col = "TimestampUTC"
    else:
        return df

    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df.dropna(subset=[ts_col])

    if df.empty:
        return df

    series = df[ts_col]
    is_tz_aware = getattr(series.dt, "tz", None) is not None

    if start_str.strip():
        start_dt = pd.to_datetime(start_str.strip(), format="%Y-%m-%d %H:%M", errors="raise")
        if is_tz_aware:
            start_dt = start_dt.tz_localize(series.dt.tz)
        df = df[df[ts_col] >= start_dt]

    if end_str.strip():
        end_dt = pd.to_datetime(end_str.strip(), format="%Y-%m-%d %H:%M", errors="raise")
        if is_tz_aware:
            end_dt = end_dt.tz_localize(series.dt.tz)
        df = df[df[ts_col] <= end_dt]

    return df


# =========================================================
# ZIP / SQLITE HELPERS
# =========================================================

def find_files(zip_path):
    db = None
    wal = None

    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            norm_name = name.replace("\\", "/")
            if norm_name.endswith(IOS_CACHE_PATH):
                db = name
            elif norm_name.endswith(IOS_WAL_PATH):
                wal = name

    return db, wal


def extract_files(zip_path, db, wal, output_folder):
    with zipfile.ZipFile(zip_path) as z:
        db_path = z.extract(db, output_folder)
        db_path_final = os.path.join(output_folder, "Cache.sqlite")

        if os.path.abspath(db_path) != os.path.abspath(db_path_final):
            if os.path.exists(db_path_final):
                os.remove(db_path_final)
            os.replace(db_path, db_path_final)

        wal_path_final = None
        if wal:
            wal_path = z.extract(wal, output_folder)
            wal_path_final = os.path.join(output_folder, "Cache.sqlite-wal")

            if os.path.abspath(wal_path) != os.path.abspath(wal_path_final):
                if os.path.exists(wal_path_final):
                    os.remove(wal_path_final)
                os.replace(wal_path, wal_path_final)

    return db_path_final, wal_path_final


def query_zrtcllocationmo(db_path, tz_str="UTC"):
    rows = []

    if not os.path.exists(db_path):
        return rows

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ZRTCLLOCATIONMO'")
    if not cursor.fetchone():
        conn.close()
        return rows

    cursor.execute("PRAGMA table_info(ZRTCLLOCATIONMO)")
    cols = [c[1] for c in cursor.fetchall()]

    required_cols = [
        "ZLATITUDE",
        "ZLONGITUDE",
        "ZHORIZONTALACCURACY",
        "ZTIMESTAMP",
        "ZSPEED",
        "ZSPEEDACCURACY",
    ]

    for col in required_cols:
        if col not in cols:
            conn.close()
            return rows

    cursor.execute("""
        SELECT
            ZLATITUDE,
            ZLONGITUDE,
            ZHORIZONTALACCURACY,
            ZTIMESTAMP,
            ZSPEED,
            ZSPEEDACCURACY
        FROM ZRTCLLOCATIONMO
    """)

    for lat, lon, acc, ts, speed, speed_acc in cursor.fetchall():
        if lat is None or lon is None:
            continue

        ts_utc = apple_to_utc(ts) if ts is not None else ""
        ts_local = convert_timezone(ts_utc, tz_str)

        rows.append({
            "TimestampUTC": ts_utc,
            "Timestamp_Local": ts_local,
            "Timezone": tz_str,
            "Latitude": lat,
            "Longitude": lon,
            "HorizontalAccuracy": acc if acc is not None else "",
            "UNITS": "meters",
            "Speed (m/s)": speed if speed is not None else "",
            "Speed Accuracy (m/s)": speed_acc if speed_acc is not None else "",
            "Speed (MPH)": mps_to_mph(speed) if speed is not None else "",
            "Speed Accuracy (MPH)": mps_to_mph(speed_acc) if speed_acc is not None else "",
        })

    conn.close()
    return rows


# =========================================================
# EXPORT HELPERS
# =========================================================

def write_csv_from_df(df, out_file):
    headers = [
        "TimestampUTC",
        "Timestamp_Local",
        "Timezone",
        "Latitude",
        "Longitude",
        "HorizontalAccuracy",
        "UNITS",
        "Speed (m/s)",
        "Speed Accuracy (m/s)",
        "Speed (MPH)",
        "Speed Accuracy (MPH)",
    ]

    df = normalize_columns(df).copy()

    for h in headers:
        if h not in df.columns:
            df[h] = ""

    df = df[headers]
    df.to_csv(out_file, index=False)


def write_kml(rows, out_file):
    kml = simplekml.Kml()

    for row in rows:
        try:
            p = kml.newpoint(
                name=str(row.get("Timestamp_Local", "") or row.get("TimestampUTC", "")),
                coords=[(float(row["Longitude"]), float(row["Latitude"]))]
            )
            p.description = (
                f"TimestampUTC: {row.get('TimestampUTC', '')}\n"
                f"Timestamp_Local: {row.get('Timestamp_Local', '')}\n"
                f"Timezone: {row.get('Timezone', '')}\n"
                f"HorizontalAccuracy: {row.get('HorizontalAccuracy', '')} meters\n"
                f"Speed (m/s): {row.get('Speed (m/s)', '')}\n"
                f"Speed Accuracy (m/s): {row.get('Speed Accuracy (m/s)', '')}\n"
                f"Speed (MPH): {row.get('Speed (MPH)', '')}\n"
                f"Speed Accuracy (MPH): {row.get('Speed Accuracy (MPH)', '')}"
            )
        except Exception:
            continue

    kml.save(out_file)


# =========================================================
# MAPPING HELPERS
# =========================================================

def add_accuracy_circle(map_obj, lat, lon, accuracy):
    acc = safe_float(accuracy)
    if acc is None or acc <= 0:
        return

    folium.Circle(
        location=[lat, lon],
        radius=acc,
        color="blue",
        weight=1,
        fill=True,
        fill_opacity=0.08
    ).add_to(map_obj)


def write_interactive_map(df, out_file):
    if df.empty:
        raise ValueError("No data available for mapping.")

    df = normalize_columns(df).copy()

    if "Latitude" not in df.columns or "Longitude" not in df.columns:
        raise ValueError("Latitude/Longitude columns not found in data.")

    df = df.dropna(subset=["Latitude", "Longitude"])
    if df.empty:
        raise ValueError("No valid coordinates found.")

    if "Timestamp_Local" in df.columns:
        df = df.sort_values("Timestamp_Local")
    elif "TimestampUTC" in df.columns:
        df = df.sort_values("TimestampUTC")

    center_lat = float(df.iloc[0]["Latitude"])
    center_lon = float(df.iloc[0]["Longitude"])

    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)
    marker_cluster = MarkerCluster().add_to(m)

    coords = []

    for _, row in df.iterrows():
        lat = safe_float(row["Latitude"])
        lon = safe_float(row["Longitude"])
        if lat is None or lon is None:
            continue

        coords.append((lat, lon))

        popup_text = (
            f"Timestamp_Local: {row.get('Timestamp_Local', '')}<br>"
            f"TimestampUTC: {row.get('TimestampUTC', '')}<br>"
            f"HorizontalAccuracy: {row.get('HorizontalAccuracy', '')} meters<br>"
            f"Speed (m/s): {row.get('Speed (m/s)', '')}<br>"
            f"Speed (MPH): {row.get('Speed (MPH)', '')}"
        )

        folium.Marker([lat, lon], popup=popup_text).add_to(marker_cluster)
        add_accuracy_circle(m, lat, lon, row.get("HorizontalAccuracy", ""))

    if len(coords) > 1:
        folium.PolyLine(coords, color="red", weight=3).add_to(m)

    m.save(out_file)


def write_timeline_map(df, out_file, fade_duration=None):
    if df.empty:
        raise ValueError("No data available for timeline mapping.")

    df = normalize_columns(df).copy()

    if "Latitude" not in df.columns or "Longitude" not in df.columns:
        raise ValueError("Latitude/Longitude columns not found in data.")

    if "Timestamp_Local" not in df.columns:
        raise ValueError("Timestamp_Local column not found in data.")

    df = df.dropna(subset=["Latitude", "Longitude", "Timestamp_Local"])
    if df.empty:
        raise ValueError("No valid coordinates/timestamps found.")

    df["Timestamp_Local"] = pd.to_datetime(df["Timestamp_Local"], errors="coerce", utc=False)
    df = df.dropna(subset=["Timestamp_Local"])
    df = df.sort_values("Timestamp_Local")

    center_lat = safe_float(df.iloc[0]["Latitude"])
    center_lon = safe_float(df.iloc[0]["Longitude"])
    if center_lat is None or center_lon is None:
        raise ValueError("No valid map center could be determined.")

    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

    for _, row in df.iterrows():
        lat = safe_float(row["Latitude"])
        lon = safe_float(row["Longitude"])
        if lat is None or lon is None:
            continue
        add_accuracy_circle(m, lat, lon, row.get("HorizontalAccuracy", ""))

    features = []

    for _, row in df.iterrows():
        lat = safe_float(row["Latitude"])
        lon = safe_float(row["Longitude"])
        if lat is None or lon is None:
            continue

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": {
                "time": row["Timestamp_Local"].isoformat(),
                "popup": (
                    f"Timestamp_Local: {row['Timestamp_Local']}<br>"
                    f"TimestampUTC: {row.get('TimestampUTC', '')}<br>"
                    f"HorizontalAccuracy: {row.get('HorizontalAccuracy', '')} meters<br>"
                    f"Speed (MPH): {row.get('Speed (MPH)', '')}"
                )
            }
        })

    if not features:
        raise ValueError("No timeline features could be created.")

    kwargs = {
        "data": {"type": "FeatureCollection", "features": features},
        "period": "PT1M",
        "add_last_point": True,
        "auto_play": False,
        "loop": False
    }

    if fade_duration is not None:
        kwargs["duration"] = fade_duration

    TimestampedGeoJson(**kwargs).add_to(m)
    m.save(out_file)


# =========================================================
# GUI
# =========================================================

class RoutinedInvestigatorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("iOS Routined Investigator Tool")
        self.root.geometry("1000x860")

        self.files = []
        self.output = None
        self.last_csv = None
        self.current_df = None

        self.build_logo()
        self.build_controls()
        self.build_log()

    def build_logo(self):
        try:
            logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
            if os.path.exists(logo_path):
                img = Image.open(logo_path)
                img = img.resize((300, 200))
                self.logo_img = ImageTk.PhotoImage(img)
                tk.Label(self.root, image=self.logo_img).pack(pady=5)
            else:
                tk.Label(self.root, text="Logo not found").pack(pady=5)
        except Exception:
            tk.Label(self.root, text="Logo could not be loaded").pack(pady=5)

    def build_controls(self):
        top = tk.Frame(self.root)
        top.pack(pady=8)

        tk.Button(top, text="Add ZIP Container", command=self.add_files, width=18).grid(row=0, column=0, padx=5, pady=5)
        tk.Button(top, text="Select Output Folder", command=self.select_output, width=18).grid(row=0, column=1, padx=5, pady=5)
        tk.Button(top, text="Start Parsing", command=self.start_parsing, width=18).grid(row=0, column=2, padx=5, pady=5)

        tz_frame = tk.Frame(self.root)
        tz_frame.pack(pady=5)

        tk.Label(tz_frame, text="Select Timezone Offset:").pack(side=tk.LEFT)
        self.timezone_var = tk.StringVar(value="UTC")
        offsets = ["UTC"] + [f"UTC{h:+d}" for h in range(-12, 15)]
        self.timezone_dropdown = ttk.Combobox(
            tz_frame,
            values=offsets,
            textvariable=self.timezone_var,
            width=8,
            state="readonly"
        )
        self.timezone_dropdown.pack(side=tk.LEFT, padx=6)

        datetime_frame = tk.Frame(self.root)
        datetime_frame.pack(pady=5)

        tk.Label(datetime_frame, text="Start (YYYY-MM-DD HH:MM):").pack(side=tk.LEFT)
        self.start_date_var = tk.StringVar()
        tk.Entry(datetime_frame, textvariable=self.start_date_var, width=18).pack(side=tk.LEFT, padx=5)

        tk.Label(datetime_frame, text="End (YYYY-MM-DD HH:MM):").pack(side=tk.LEFT)
        self.end_date_var = tk.StringVar()
        tk.Entry(datetime_frame, textvariable=self.end_date_var, width=18).pack(side=tk.LEFT, padx=5)

        fade_frame = tk.Frame(self.root)
        fade_frame.pack(pady=5)

        tk.Label(fade_frame, text="Timeline Fade Duration:").pack(side=tk.LEFT)
        self.fade_var = tk.StringVar(value="None")
        fade_options = [
            "None",
            "5 minutes",
            "15 minutes",
            "30 minutes",
            "1 hour",
            "6 hours",
            "12 hours",
            "1 day"
        ]
        self.fade_dropdown = ttk.Combobox(
            fade_frame,
            values=fade_options,
            textvariable=self.fade_var,
            width=12,
            state="readonly"
        )
        self.fade_dropdown.pack(side=tk.LEFT, padx=6)

        fmt_frame = tk.Frame(self.root)
        fmt_frame.pack(pady=5)

        tk.Label(fmt_frame, text="Output Format:").pack(side=tk.LEFT)
        self.output_csv_var = tk.BooleanVar(value=True)
        self.output_kml_var = tk.BooleanVar(value=False)
        tk.Checkbutton(fmt_frame, text="CSV", variable=self.output_csv_var).pack(side=tk.LEFT, padx=5)
        tk.Checkbutton(fmt_frame, text="KML", variable=self.output_kml_var).pack(side=tk.LEFT, padx=5)

        map_frame = tk.Frame(self.root)
        map_frame.pack(pady=8)

        tk.Button(map_frame, text="Load CSV for Mapping", command=self.load_csv_for_mapping, width=20).grid(row=0, column=0, padx=5, pady=5)
        tk.Button(map_frame, text="Interactive Map", command=self.create_map, width=20).grid(row=0, column=1, padx=5, pady=5)
        tk.Button(map_frame, text="Timeline Map", command=self.create_timeline_map, width=20).grid(row=0, column=2, padx=5, pady=5)

        self.progress = ttk.Progressbar(self.root, length=920)
        self.progress.pack(pady=10)

    def build_log(self):
        self.log = ScrolledText(self.root, height=28)
        self.log.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)

    def log_msg(self, msg):
        self.log.insert(tk.END, msg + "\n")
        self.log.see(tk.END)
        self.root.update()

    def add_files(self):
        files = filedialog.askopenfilenames(filetypes=[("ZIP Files", "*.zip")])
        for f in files:
            if f not in self.files:
                self.files.append(f)
                self.log_msg(f"Added: {f}")

    def select_output(self):
        self.output = filedialog.askdirectory()
        if self.output:
            self.log_msg(f"Output folder: {self.output}")

    def get_filtered_df(self):
        df = self.get_df_for_mapping()

        start_dt = self.start_date_var.get().strip()
        end_dt = self.end_date_var.get().strip()

        if not start_dt and not end_dt:
            return df

        try:
            filtered_df = apply_datetime_filter(df, start_dt, end_dt)
            return filtered_df
        except Exception as e:
            raise ValueError(f"Invalid date/time filter. Use YYYY-MM-DD HH:MM.\n\n{e}")

    def start_parsing(self):
        if not self.files or not self.output:
            messagebox.showwarning("Missing Input", "Please add ZIP file(s) and select an output folder.")
            return

        if not self.output_csv_var.get() and not self.output_kml_var.get():
            messagebox.showwarning("No Output Selected", "Select CSV, KML, or both.")
            return

        self.progress["maximum"] = len(self.files)
        tz_str = self.timezone_var.get()

        for i, f in enumerate(self.files, start=1):
            self.log_msg(f"Processing: {f}")

            db, wal = find_files(f)
            if not db:
                self.log_msg("Cache.sqlite not found at expected iOS path in ZIP.")
                continue

            try:
                db_path, wal_path = extract_files(f, db, wal, self.output)
                self.log_msg(f"Extracted DB: {db_path}")
                if wal_path:
                    self.log_msg(f"Extracted WAL: {wal_path}")

                rows = query_zrtcllocationmo(db_path, tz_str=tz_str)

                if not rows:
                    self.log_msg("No ZRTCLLOCATIONMO data found.")
                    continue

                parsed_df = pd.DataFrame(rows)
                parsed_df = normalize_columns(parsed_df)

                try:
                    parsed_df = apply_datetime_filter(
                        parsed_df,
                        self.start_date_var.get().strip(),
                        self.end_date_var.get().strip()
                    )
                except Exception as e:
                    messagebox.showerror("Date/Time Filter Error", str(e))
                    return

                if parsed_df.empty:
                    self.log_msg("No records remained after date/time filtering.")
                    continue

                base = os.path.splitext(os.path.basename(f))[0]
                csv_file = os.path.join(self.output, base + "_ZRTCLLOCATIONMO.csv")
                kml_file = os.path.join(self.output, base + "_ZRTCLLOCATIONMO.kml")

                self.current_df = parsed_df

                if self.output_csv_var.get():
                    write_csv_from_df(parsed_df, csv_file)
                    self.log_msg(f"CSV saved: {csv_file}")
                    self.last_csv = csv_file

                if self.output_kml_var.get():
                    kml_rows = parsed_df.to_dict(orient="records")
                    write_kml(kml_rows, kml_file)
                    self.log_msg(f"KML saved: {kml_file}")

            except Exception as e:
                self.log_msg(f"Error: {e}")

            self.progress["value"] = i
            self.root.update()

        self.log_msg("Processing Complete.")

    def load_csv_for_mapping(self):
        csv_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if not csv_path:
            return

        try:
            df = pd.read_csv(csv_path)
            df = normalize_columns(df)
            self.current_df = df
            self.last_csv = csv_path
            self.log_msg(f"Loaded CSV for mapping: {csv_path}")
            self.log_msg(f"Columns: {list(df.columns)}")
        except Exception as e:
            messagebox.showerror("Load Error", str(e))

    def get_df_for_mapping(self):
        if self.current_df is not None:
            return self.current_df

        if self.last_csv and os.path.exists(self.last_csv):
            df = pd.read_csv(self.last_csv)
            df = normalize_columns(df)
            self.current_df = df
            return df

        raise ValueError("No parsed CSV is loaded. Parse a ZIP or load a CSV first.")

    def create_map(self):
        try:
            df = self.get_filtered_df()
            if df.empty:
                raise ValueError("No records match the selected date/time filter.")

            out_file = os.path.join(self.output if self.output else os.getcwd(), "investigator_map.html")
            write_interactive_map(df, out_file)
            self.log_msg(f"Interactive map saved: {out_file}")
            webbrowser.open(out_file)
        except Exception as e:
            messagebox.showerror("Map Error", str(e))

    def create_timeline_map(self):
        try:
            df = self.get_filtered_df()
            if df.empty:
                raise ValueError("No records match the selected date/time filter.")

            fade_duration = get_fade_duration_iso(self.fade_var.get())

            out_file = os.path.join(self.output if self.output else os.getcwd(), "timeline_map.html")
            write_timeline_map(df, out_file, fade_duration=fade_duration)
            self.log_msg(f"Timeline map saved: {out_file}")
            webbrowser.open(out_file)
        except Exception as e:
            messagebox.showerror("Timeline Error", str(e))


# =========================================================
# RUN
# =========================================================

def main():
    root = tk.Tk()
    app = RoutinedInvestigatorGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

