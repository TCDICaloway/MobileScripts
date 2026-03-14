import os
import zipfile
import sqlite3
import webbrowser
from datetime import datetime, timedelta, timezone

import tkinter as tk
from tkinter import filedialog, ttk, messagebox
from tkinter.scrolledtext import ScrolledText
from tkcalendar import DateEntry

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

STANDARD_COLUMNS = [
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

def apply_datetime_filter(df, start_str="", end_str=""):
    """
    Filters dataframe using Timestamp_Local first, then TimestampUTC.
    Format must be YYYY-MM-DD HH:MM
    Handles timezone-aware and timezone-naive timestamps safely.
    """
    if df is None or df.empty:
        return df

    df = df.copy()

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


def standardize_user_csv(df_raw, tz_choice="UTC", column_map=None):
    """
    Build a standardized dataframe using user-selected columns.
    """
    df_raw = df_raw.copy()
    df_out = pd.DataFrame()

    column_map = column_map or {}

    lat_src = column_map.get("Latitude")
    lon_src = column_map.get("Longitude")
    ts_local_src = column_map.get("Timestamp_Local")
    ts_utc_src = column_map.get("TimestampUTC")
    acc_src = column_map.get("HorizontalAccuracy")
    speed_src = column_map.get("Speed (m/s)")
    speed_acc_src = column_map.get("Speed Accuracy (m/s)")

    if not lat_src or not lon_src:
        raise ValueError("Latitude and Longitude must be selected.")

    df_out["Latitude"] = df_raw[lat_src]
    df_out["Longitude"] = df_raw[lon_src]

    if ts_local_src:
        df_out["Timestamp_Local"] = df_raw[ts_local_src]
    else:
        df_out["Timestamp_Local"] = ""

    if ts_utc_src:
        df_out["TimestampUTC"] = df_raw[ts_utc_src]
    else:
        df_out["TimestampUTC"] = ""

    if ts_local_src and not ts_utc_src:
        df_out["Timestamp_Local"] = pd.to_datetime(df_out["Timestamp_Local"], errors="coerce")
        df_out["TimestampUTC"] = df_out["Timestamp_Local"].astype(str)
    elif ts_utc_src and not ts_local_src:
        df_out["TimestampUTC"] = pd.to_datetime(df_out["TimestampUTC"], errors="coerce")
        df_out["Timestamp_Local"] = df_out["TimestampUTC"].astype(str)

    df_out["Timezone"] = tz_choice

    if acc_src:
        df_out["HorizontalAccuracy"] = df_raw[acc_src]
    else:
        df_out["HorizontalAccuracy"] = ""

    df_out["UNITS"] = "meters"

    if speed_src:
        df_out["Speed (m/s)"] = df_raw[speed_src]
    else:
        df_out["Speed (m/s)"] = ""

    if speed_acc_src:
        df_out["Speed Accuracy (m/s)"] = df_raw[speed_acc_src]
    else:
        df_out["Speed Accuracy (m/s)"] = ""

    df_out["Speed (MPH)"] = df_out["Speed (m/s)"].apply(mps_to_mph)
    df_out["Speed Accuracy (MPH)"] = df_out["Speed Accuracy (m/s)"].apply(mps_to_mph)

    for col in STANDARD_COLUMNS:
        if col not in df_out.columns:
            df_out[col] = ""

    df_out = df_out[STANDARD_COLUMNS]
    return df_out


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
    df = df.copy()
    for h in STANDARD_COLUMNS:
        if h not in df.columns:
            df[h] = ""
    df = df[STANDARD_COLUMNS]
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

    if "Latitude" not in df.columns or "Longitude" not in df.columns:
        raise ValueError("Latitude/Longitude columns not found in data.")

    if "Timestamp_Local" not in df.columns:
        raise ValueError("Timestamp_Local column not found in data.")

    df = df.copy()
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

    features = []

    for _, row in df.iterrows():
        lat = safe_float(row["Latitude"])
        lon = safe_float(row["Longitude"])
        acc = safe_float(row.get("HorizontalAccuracy", ""))

        if lat is None or lon is None:
            continue

        timestamp_local = row["Timestamp_Local"]
        popup_html = (
            f"Timestamp_Local: {timestamp_local}<br>"
            f"TimestampUTC: {row.get('TimestampUTC', '')}<br>"
            f"HorizontalAccuracy: {row.get('HorizontalAccuracy', '')} meters<br>"
            f"Speed (MPH): {row.get('Speed (MPH)', '')}"
        )

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": {
                "time": timestamp_local.isoformat(),
                "popup": popup_html,
                "icon": "circle",
                "iconstyle": {
                    "fillColor": "red",
                    "fillOpacity": 0.85,
                    "stroke": True,
                    "radius": 6
                }
            }
        })

        # Add the accuracy visualization to the same timeline layer so it fades too.
        # TimestampedGeoJson circle sizes are pixel-based, so this is a visual estimate,
        # not a true meter-based geographic radius like folium.Circle.
        if acc is not None and acc > 0:
            pixel_radius = max(3, min(acc / 2, 60))
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "properties": {
                    "time": timestamp_local.isoformat(),
                    "popup": popup_html,
                    "icon": "circle",
                    "iconstyle": {
                        "fillColor": "blue",
                        "fillOpacity": 0.08,
                        "color": "blue",
                        "stroke": True,
                        "weight": 1,
                        "radius": pixel_radius
                    }
                }
            })

    if not features:
        raise ValueError("No timeline features could be created.")

    kwargs = {
        "data": {"type": "FeatureCollection", "features": features},
        "period": "PT1M",
        "add_last_point": False,
        "auto_play": False,
        "loop": False,
        "max_speed": 10,
        "loop_button": True,
        "date_options": "YYYY-MM-DD HH:mm:ss",
        "time_slider_drag_update": True
    }

    if fade_duration is not None:
        kwargs["duration"] = fade_duration

    TimestampedGeoJson(**kwargs).add_to(m)
    m.save(out_file)


# =========================================================
# COLUMN MAPPING DIALOG
# =========================================================

class ColumnMappingDialog:
    def __init__(self, parent, columns):
        self.top = tk.Toplevel(parent)
        self.top.title("Map CSV Columns")
        self.top.geometry("520x360")
        self.top.grab_set()

        self.result = None
        self.columns = [""] + list(columns)

        self.vars = {
            "Latitude": tk.StringVar(),
            "Longitude": tk.StringVar(),
            "Timestamp_Local": tk.StringVar(),
            "TimestampUTC": tk.StringVar(),
            "HorizontalAccuracy": tk.StringVar(),
            "Speed (m/s)": tk.StringVar(),
            "Speed Accuracy (m/s)": tk.StringVar(),
        }

        row = 0
        tk.Label(self.top, text="Choose which CSV column maps to each item.", font=("Arial", 11, "bold")).grid(row=row, column=0, columnspan=2, pady=10)
        row += 1

        for label in self.vars:
            tk.Label(self.top, text=label + ":").grid(row=row, column=0, sticky="e", padx=8, pady=5)
            ttk.Combobox(
                self.top,
                textvariable=self.vars[label],
                values=self.columns,
                state="readonly",
                width=32
            ).grid(row=row, column=1, sticky="w", padx=8, pady=5)
            row += 1

        btn_frame = tk.Frame(self.top)
        btn_frame.grid(row=row, column=0, columnspan=2, pady=15)

        tk.Button(btn_frame, text="OK", width=12, command=self.on_ok).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Cancel", width=12, command=self.on_cancel).pack(side=tk.LEFT, padx=5)

    def on_ok(self):
        if not self.vars["Latitude"].get() or not self.vars["Longitude"].get():
            messagebox.showerror("Missing Columns", "Latitude and Longitude must be selected.", parent=self.top)
            return

        self.result = {k: v.get() for k, v in self.vars.items() if v.get()}
        self.top.destroy()

    def on_cancel(self):
        self.result = None
        self.top.destroy()


# =========================================================
# GUI
# =========================================================

class RoutinedInvestigatorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("iOS Routined Investigator Tool")
        self.root.geometry("1040x930")

        self.files = []
        self.output = None
        self.last_csv = None
        self.current_df = None
        self.column_map = {}

        self.build_logo()
        self.build_controls()
        self.build_log()

    def build_logo(self):
        try:
            logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
            if os.path.exists(logo_path):
                img = Image.open(logo_path)
                img = img.resize((400, 300))
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

        # Date / time filter controls
        datetime_frame = tk.Frame(self.root)
        datetime_frame.pack(pady=5)

        tk.Label(datetime_frame, text="Start Date/Time:").grid(row=0, column=0, padx=5, pady=4, sticky="e")
        self.start_date_picker = DateEntry(datetime_frame, width=12, date_pattern="yyyy-mm-dd")
        self.start_date_picker.grid(row=0, column=1, padx=5, pady=4)

        self.start_hour_var = tk.StringVar(value="00")
        self.start_hour_dropdown = ttk.Combobox(
            datetime_frame,
            textvariable=self.start_hour_var,
            values=[f"{h:02d}" for h in range(24)],
            width=3,
            state="readonly"
        )
        self.start_hour_dropdown.grid(row=0, column=2, padx=2, pady=4)

        tk.Label(datetime_frame, text=":").grid(row=0, column=3, padx=0, pady=4)

        self.start_minute_var = tk.StringVar(value="00")
        self.start_minute_dropdown = ttk.Combobox(
            datetime_frame,
            textvariable=self.start_minute_var,
            values=[f"{m:02d}" for m in range(60)],
            width=3,
            state="readonly"
        )
        self.start_minute_dropdown.grid(row=0, column=4, padx=2, pady=4)

        tk.Label(datetime_frame, text="End Date/Time:").grid(row=0, column=5, padx=10, pady=4, sticky="e")
        self.end_date_picker = DateEntry(datetime_frame, width=12, date_pattern="yyyy-mm-dd")
        self.end_date_picker.grid(row=0, column=6, padx=5, pady=4)

        self.end_hour_var = tk.StringVar(value="23")
        self.end_hour_dropdown = ttk.Combobox(
            datetime_frame,
            textvariable=self.end_hour_var,
            values=[f"{h:02d}" for h in range(24)],
            width=3,
            state="readonly"
        )
        self.end_hour_dropdown.grid(row=0, column=7, padx=2, pady=4)

        tk.Label(datetime_frame, text=":").grid(row=0, column=8, padx=0, pady=4)

        self.end_minute_var = tk.StringVar(value="59")
        self.end_minute_dropdown = ttk.Combobox(
            datetime_frame,
            textvariable=self.end_minute_var,
            values=[f"{m:02d}" for m in range(60)],
            width=3,
            state="readonly"
        )
        self.end_minute_dropdown.grid(row=0, column=9, padx=2, pady=4)

        self.enable_date_filter_var = tk.BooleanVar(value=False)
        tk.Checkbutton(
            datetime_frame,
            text="Enable Filter",
            variable=self.enable_date_filter_var
        ).grid(row=0, column=10, padx=10, pady=4)

        tk.Button(datetime_frame, text="Clear Filter", command=self.clear_filter, width=12).grid(row=0, column=11, padx=8, pady=4)

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

        self.progress = ttk.Progressbar(self.root, length=960)
        self.progress.pack(pady=10)

    def build_log(self):
        self.log = ScrolledText(self.root, height=30)
        self.log.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)

    def log_msg(self, msg):
        self.log.insert(tk.END, msg + "\n")
        self.log.see(tk.END)
        self.root.update()

    def get_start_datetime_string(self):
        if not self.enable_date_filter_var.get():
            return ""
        return f"{self.start_date_picker.get()} {self.start_hour_var.get()}:{self.start_minute_var.get()}"

    def get_end_datetime_string(self):
        if not self.enable_date_filter_var.get():
            return ""
        return f"{self.end_date_picker.get()} {self.end_hour_var.get()}:{self.end_minute_var.get()}"

    def clear_filter(self):
        today = datetime.now().date()
        self.start_date_picker.set_date(today)
        self.end_date_picker.set_date(today)
        self.start_hour_var.set("00")
        self.start_minute_var.set("00")
        self.end_hour_var.set("23")
        self.end_minute_var.set("59")
        self.enable_date_filter_var.set(False)
        self.log_msg("Date/time filter cleared.")

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

        start_dt = self.get_start_datetime_string()
        end_dt = self.get_end_datetime_string()

        if not start_dt and not end_dt:
            return df

        try:
            return apply_datetime_filter(df, start_dt, end_dt)
        except Exception as e:
            raise ValueError(f"Invalid date/time filter.\n\n{e}")

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

                parsed_df = apply_datetime_filter(
                    parsed_df,
                    self.get_start_datetime_string(),
                    self.get_end_datetime_string()
                )

                if parsed_df.empty:
                    self.log_msg("No records remained after date/time filtering.")
                    continue

                base = os.path.splitext(os.path.basename(f))[0]
                csv_file = os.path.join(self.output, base + "_ZRTCLLOCATIONMO.csv")
                kml_file = os.path.join(self.output, base + "_ZRTCLLOCATIONMO.kml")

                self.current_df = parsed_df
                self.column_map = {}

                if self.output_csv_var.get():
                    write_csv_from_df(parsed_df, csv_file)
                    self.log_msg(f"CSV saved: {csv_file}")
                    self.last_csv = csv_file

                if self.output_kml_var.get():
                    write_kml(parsed_df.to_dict(orient="records"), kml_file)
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
            raw_df = pd.read_csv(csv_path)

            dialog = ColumnMappingDialog(self.root, raw_df.columns)
            self.root.wait_window(dialog.top)

            if dialog.result is None:
                self.log_msg("CSV column mapping cancelled.")
                return

            self.column_map = dialog.result
            standardized_df = standardize_user_csv(
                raw_df,
                tz_choice=self.timezone_var.get(),
                column_map=self.column_map
            )

            self.current_df = standardized_df
            self.last_csv = csv_path

            self.log_msg(f"Loaded CSV for mapping: {csv_path}")
            self.log_msg(f"User column map: {self.column_map}")
            self.log_msg(f"Standardized columns: {list(standardized_df.columns)}")

        except Exception as e:
            messagebox.showerror("Load Error", str(e))

    def get_df_for_mapping(self):
        if self.current_df is not None:
            return self.current_df

        if self.last_csv and os.path.exists(self.last_csv):
            raw_df = pd.read_csv(self.last_csv)
            if self.column_map:
                self.current_df = standardize_user_csv(
                    raw_df,
                    tz_choice=self.timezone_var.get(),
                    column_map=self.column_map
                )
            else:
                self.current_df = raw_df
            return self.current_df

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