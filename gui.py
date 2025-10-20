import tkinter as tk
from tkinter import ttk, messagebox
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
from backtest import fetch_ohlcv, run_backtest, save_results_to_db

def start_backtest():
    symbol = symbol_entry.get()
    intervall = intervall_entry.get()
    category = category_entry.get()
    days = int(days_entry.get())
    session_start = session_start_entry.get()
    session_end = session_end_entry.get()

    def progress_callback(value):
        progress_var.set(value)
        root.update_idletasks()

    df = fetch_ohlcv(symbol, intervall, category, days, progress_callback)
    result = run_backtest(df, session_start, session_end)

    info_labels["days"].config(text=f"Untersuchte Tage: {result['count_days']}")
    info_labels["hits"].config(text=f"Treffer: {result['count_hits']}")
    info_labels["ratio"].config(text=f"Trefferquote: {result['count_hits']/result['count_days']*100:.2f}%")
    info_labels["start"].config(text=f"Startdatum: {result['daily'].index[0].date()}")
    info_labels["end"].config(text=f"Enddatum: {result['daily'].index[-1].date()}")
    info_labels["last_high_low"].config(text=f"Letzter Tag H/L: {result['daily']['high'].iloc[-1]:.2f}/{result['daily']['low'].iloc[-1]:.2f}")

    for row in results_table.get_children():
        results_table.delete(row)
    for row in result["records"]:
        results_table.insert("", "end", values=(
            row["Date"],
            row["Touched High"], f"{row['High Price']:.2f}",
            row["Touched Low"], f"{row['Low Price']:.2f}",
            f"{row['Close Price']:.2f}"
        ))

    backtest_id = save_results_to_db(
        result["records"], 
        symbol, 
        intervall, 
        category, 
        session_start, 
        session_end,
        result
    )
    messagebox.showinfo("Backtest abgeschlossen", f"Backtest-ID {backtest_id} wurde in der Datenbank gespeichert.")

# --- GUI Setup ---
root = tk.Tk()
root.title("BTC Backtester High/Low")
root.geometry("1100x750")

frame_inputs = tk.Frame(root)
frame_inputs.pack(pady=10)
tk.Label(frame_inputs, text="Symbol").grid(row=0,column=0)
symbol_entry = tk.Entry(frame_inputs); symbol_entry.insert(0,"BTCUSDT"); symbol_entry.grid(row=0,column=1)
tk.Label(frame_inputs, text="Intervall").grid(row=0,column=2)
intervall_entry = tk.Entry(frame_inputs); intervall_entry.insert(0,"60"); intervall_entry.grid(row=0,column=3)
tk.Label(frame_inputs, text="Category").grid(row=0,column=4)
category_entry = tk.Entry(frame_inputs); category_entry.insert(0,"spot"); category_entry.grid(row=0,column=5)
tk.Label(frame_inputs, text="Session Start").grid(row=1,column=0)
session_start_entry = tk.Entry(frame_inputs); session_start_entry.insert(0,"15:30:00"); session_start_entry.grid(row=1,column=1)
tk.Label(frame_inputs, text="Session End").grid(row=1,column=2)
session_end_entry = tk.Entry(frame_inputs); session_end_entry.insert(0,"22:00:00"); session_end_entry.grid(row=1,column=3)
tk.Label(frame_inputs, text="Tage zurück").grid(row=1,column=4)
days_entry = tk.Entry(frame_inputs); days_entry.insert(0,"730"); days_entry.grid(row=1,column=5)

progress_var = tk.DoubleVar()
progress_bar = ttk.Progressbar(root, variable=progress_var, maximum=100, length=500)
progress_bar.pack(pady=10)

frame_info = tk.Frame(root)
frame_info.pack(pady=10)
info_labels = {}
for idx, text in enumerate(["days","hits","ratio","start","end","last_high_low"]):
    lbl = tk.Label(frame_info, text=text+":"); lbl.grid(row=0,column=idx); info_labels[text]=lbl

frame_table = tk.Frame(root); frame_table.pack(pady=10)
results_table = ttk.Treeview(frame_table, columns=("Date","Touched High","High Price","Touched Low","Low Price","Close Price"), show='headings')
results_table.heading("Date", text="Datum")
results_table.heading("Touched High", text="High getriggert")
results_table.heading("High Price", text="High Preis")
results_table.heading("Touched Low", text="Low getriggert")
results_table.heading("Low Price", text="Low Preis")
results_table.heading("Close Price", text="Schlusskurs")
results_table.pack()

start_button = tk.Button(root, text="Backtest starten", command=start_backtest)
start_button.pack(pady=10)

root.mainloop()