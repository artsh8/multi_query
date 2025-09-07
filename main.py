import queue
from typing import Any, NamedTuple
from enum import IntEnum
import atexit
import threading
import json
import re
import tkinter as tk
from tkinter import messagebox
import ttkbootstrap as ttk
import xlsxwriter
from db import new_db, DB, DBError, Storage
from highlighting import SQL_PATTERN, MONGO_PATTERN


class Syntax(IntEnum):
    SQL = 0
    MONGO = 1


class ScrollableTab(ttk.Frame):

    def __init__(self, parent: ttk.Notebook):
        super().__init__(parent)
        self.canvas = ttk.Canvas(self)
        self.scrollbar = ttk.Scrollbar(
            self, orient="vertical", command=self.canvas.yview
        )
        self.scrollbar_frame = ttk.Frame(self.canvas)
        self.scrollbar_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")),
        )
        self.canvas.create_window((0, 0), window=self.scrollbar_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")


class SyntaxHighlighter:

    def __init__(self, parent: ttk.Frame):
        self.parent = parent
        self.frame = ttk.Frame(parent)
        self.highlight_choice = tk.IntVar(value=Syntax.SQL)
        radio_grid = ttk.Frame(self.frame)
        ttk.Radiobutton(
            radio_grid, text="SQL", variable=self.highlight_choice, value=Syntax.SQL
        ).grid(row=0, column=0)
        ttk.Radiobutton(
            radio_grid, text="Mongo", variable=self.highlight_choice, value=Syntax.MONGO
        ).grid(row=0, column=1)
        self.entry = ttk.Text(self.frame, wrap=tk.WORD, font=("Helvetica", 12))
        radio_grid.pack()
        self.entry.pack(expand=True, fill=tk.BOTH)
        self.entry.tag_config("highlight", foreground="blue")
        self.entry.bind("<<Modified>>", self.highlight_keywords)

    def highlight_keywords(self, event=None) -> None:
        if not self.entry.edit_modified():
            return None
        self.entry.edit_modified(False)
        self.entry.tag_remove("highlight", "1.0", "end")
        text_content = self.entry.get("1.0", "end-1c").casefold()
        lines = text_content.split("\n")
        if self.highlight_choice.get() == Syntax.SQL:
            pattern = SQL_PATTERN
        elif self.highlight_choice.get() == Syntax.MONGO:
            pattern = MONGO_PATTERN

        for word_match in re.finditer(pattern, text_content):
            start, end = word_match.start(), word_match.end()
            start_line = 1 + text_content.count("\n", 0, start)
            end_line = 1 + text_content.count("\n", 0, end)
            start_col = start - sum(len(line) + 1 for line in lines[: start_line - 1])
            end_col = end - sum(len(line) + 1 for line in lines[: end_line - 1])
            start_index = f"{start_line}.{start_col}"
            end_index = f"{end_line}.{end_col}"
            if self.is_whole_word(start_index, end_index):
                self.entry.tag_add("highlight", start_index, end_index)

    def is_whole_word(self, start_index: str, end_index: str) -> bool:
        try:
            char_before = self.entry.get(f"{start_index} -1c", start_index)
            char_after = self.entry.get(end_index, f"{end_index} +1c")
        except tk.TclError:
            char_before = char_after = ""

        return (not char_before.isalnum() and char_before != "_") and (
            not char_after.isalnum() and char_after != "_"
        )

    def is_valid_entry(self, user_entry: str, selected_stands: list[str]) -> bool:
        if len(user_entry) == 0:
            messagebox.showerror("Ошибка", "Заполните поле для запроса")
            return False
        selected_syntaxes = set(stands[selected].syntax for selected in selected_stands)
        if len(selected_syntaxes) > 1:
            messagebox.showerror("Ошибка", "Выбраны стенды с разными синтаксисами")
            return False
        chosen_syntax = self.highlight_choice.get()
        if chosen_syntax not in selected_syntaxes:
            messagebox.showerror(
                "Ошибка", "Выбранный синтаксис отличается от синтаксиса стендов"
            )
            return False
        if chosen_syntax == Syntax.SQL:
            entry_slice = user_entry[:9].casefold()
            if entry_slice.startswith(("insert ", "update ", "delete ")):
                messagebox.showerror(
                    "Ошибка", "Редактирование данных не поддерживается"
                )
                return False
            elif entry_slice.startswith(("drop ", "alter ", "create ", "comment ")):
                messagebox.showerror("Ошибка", "Редактирование схемы не поддерживается")
                return False
            elif entry_slice == "truncate ":
                messagebox.showerror("Ошибка", "Очистка таблицы не поддерживается")
                return False
        elif chosen_syntax == Syntax.MONGO:
            try:
                json.loads(user_entry)
            except json.decoder.JSONDecodeError:
                messagebox.showerror("Ошибка", "Некорректный синтаксис для поиска")
                return False
        return True


class Wgui:

    def __init__(self, win: ttk.Window):
        self.storage = Storage(INTERNAL_DB)
        notebook = ttk.Notebook(win)
        notebook.pack(padx=10, pady=10, fill="both", expand=True)
        self.stands_tab = ScrollableTab(notebook)
        query_tab = ttk.Frame(notebook)
        self.results_tab = ScrollableTab(notebook)
        notebook.add(self.stands_tab, text="Стенды")
        notebook.add(query_tab, text="Запрос")
        notebook.add(self.results_tab, text="Результаты")

        ttk.Button(
            self.stands_tab.scrollbar_frame,
            text="Выделить все",
            command=self.select_all,
            bootstyle="secondary",
        ).grid(row=0, column=0, sticky="nsew", padx=0, pady=0)
        ttk.Button(
            self.stands_tab.scrollbar_frame,
            text="Снять выделение",
            command=self.deselect_all,
            bootstyle="secondary",
        ).grid(row=1, column=0, sticky="nsew", padx=0, pady=0)
        ttk.Label(
            self.stands_tab.scrollbar_frame, text="Стенд", font=("Segoe UI", 9, "bold")
        ).grid(row=0, column=1, rowspan=2, sticky="nsew", padx=10, pady=0)
        ttk.Label(
            self.stands_tab.scrollbar_frame, text="Вид БД", font=("Segoe UI", 9, "bold")
        ).grid(row=0, column=2, rowspan=2, sticky="nsew", padx=10, pady=0)

        self.highlighted_frame = SyntaxHighlighter(query_tab)
        self.highlighted_frame.frame.pack(fill="both", expand=True)
        ex_grid = ttk.Frame(query_tab)
        ttk.Label(ex_grid, text="Лимит записей:").grid(row=0, column=0)
        self.fetch_size = ttk.Entry(ex_grid)
        self.fetch_size.insert(0, "25")
        self.fetch_size.grid(row=0, column=1)
        ttk.Button(ex_grid, text="Выполнить", command=self.create_tasks).grid(
            row=0, column=2, padx=10
        )
        ex_grid.pack(anchor="e", pady=10)

        self.results_tab.scrollbar_frame.grid_columnconfigure(1, weight=1, minsize=250)
        ttk.Button(
            self.results_tab.scrollbar_frame,
            text="Обновить",
            command=self.refresh_results,
            bootstyle="info",
        ).grid(row=0, column=0, sticky="nsew", padx=0, pady=0)
        ttk.Label(
            self.results_tab.scrollbar_frame,
            text="Дата запроса",
            font=("Segoe UI", 9, "bold"),
        ).grid(row=1, column=0, sticky="nsew", padx=0, pady=0)
        ttk.Label(
            self.results_tab.scrollbar_frame,
            text="Содержание запроса",
            font=("Segoe UI", 9, "bold"),
        ).grid(row=1, column=1, sticky="nsew", padx=0, pady=0)
        ttk.Label(
            self.results_tab.scrollbar_frame,
            text="Статус задачи",
            font=("Segoe UI", 9, "bold"),
        ).grid(row=1, column=2, sticky="nsew", padx=0, pady=0)
        self.displayed_results: list[ttk.Label | ttk.Button] = []

    @staticmethod
    def create_stands() -> None:
        global stands
        for name, connection in config.get("connections", {}).items():
            try:
                db = new_db(connection)
            except (KeyError, ValueError) as e:
                messagebox.showerror("Ошибка", f"{name}: {e}")
            else:
                stands[name] = Stand(name, connection["vendor"], db)

    def draw_stands(self) -> None:
        self.create_stands()
        for n, stand in enumerate(stands.values(), start=2):
            stand.checkbutton = ttk.Checkbutton(
                self.stands_tab.scrollbar_frame,
                variable=stand.checkbox,
                offvalue=0,
                onvalue=1,
            )
            stand.checkbutton.grid(row=n, column=0, sticky="", padx=0, pady=0)
            ttk.Label(self.stands_tab.scrollbar_frame, text=stand.name).grid(
                row=n, column=1, sticky="nsew", padx=0, pady=0
            )
            ttk.Label(self.stands_tab.scrollbar_frame, text=stand.vendor).grid(
                row=n, column=2, sticky="nsew", padx=0, pady=0
            )

    @staticmethod
    def select_all() -> None:
        for stand in stands.values():
            stand.checkbox.set(True)

    @staticmethod
    def deselect_all() -> None:
        for stand in stands.values():
            stand.checkbox.set(False)

    @staticmethod
    def is_positive_int(entry_value: str) -> bool:
        try:
            int(entry_value)
        except ValueError:
            return False
        if int(entry_value) < 1:
            return False
        return True

    @staticmethod
    def make_pivot(results: list[tuple[str, list[dict]]]) -> list[tuple[str, Any]] | None:
        if results and results[0] and (len(results[0][1]) == 1) and (len(results[0][1][0]) == 1):
            return [(r[0], next(iter(r[1][0].values()))) for r in results]
        else:
            return None

    def export_result(self, query_id: int) -> None:
        query_results = self.storage.results_by_query_id(query_id)
        query_syntax, query_content = self.storage.query_by_id(query_id)
        workbook = xlsxwriter.Workbook(f"query_{query_id}.xlsx")
        bold = workbook.add_format({"bold": True})
        query_worksheet = workbook.add_worksheet("Запрос")
        query_worksheet.write(0, 0, "Содержание запроса", bold)
        query_worksheet.write(1, 0, query_content)
        results: list[tuple[str, list[dict]]] = [
            (r[0], json.loads(r[1])) for r in query_results
        ]
        if query_syntax == Syntax.SQL:
            if pivot := self.make_pivot(results):
                worksheet = workbook.add_worksheet("Свод")
                worksheet.write(0, 0, "Стенд", bold)
                worksheet.write(0, 1, "Содержимое первой колонки", bold)
                for pivot_row_num, pivot_row_data in enumerate(pivot, start=1):
                    worksheet.write(pivot_row_num, 0, pivot_row_data[0])
                    worksheet.write(pivot_row_num, 1, str(pivot_row_data[1]))
            for sheet_name, rows in results:
                worksheet = workbook.add_worksheet(sheet_name)
                if not rows:
                    headers: Any = ("Данные отсутствуют",)
                else:
                    headers = rows[0].keys()
                for col_num, header in enumerate(headers):
                    worksheet.write(0, col_num, header, bold)
                for row_num, row_data in enumerate(rows, start=1):
                    for col_num, value in enumerate(row_data.values()):
                        worksheet.write(row_num, col_num, str(value))
        elif query_syntax == Syntax.MONGO:
            for sheet_name, rows in results:
                worksheet = workbook.add_worksheet(sheet_name)
                worksheet.write(0, 0, "_id", bold)
                worksheet.write(0, 1, "Данные", bold)
                for row_num, row_data in enumerate(rows, start=1):
                    worksheet.write(row_num, 0, row_data.get("_id", None))
                    worksheet.write(row_num, 1, str(row_data))

        try:
            workbook.close()
        except xlsxwriter.exceptions.FileCreateError:
            messagebox.showerror(
                "Ошибка", "Файл для записи результата должен быть закрыт"
            )

    def refresh_results(self) -> None:
        for r in self.displayed_results:
            r.destroy()
        self.displayed_results.clear()
        latest_queries = self.storage.latest_queries()
        for n, query in enumerate(latest_queries, start=2):
            query_id, created_at, content, progress = query
            created_at_l = ttk.Label(
                self.results_tab.scrollbar_frame,
                text=created_at,
                borderwidth=1,
                relief="ridge",
            )
            created_at_l.grid(
                row=n,
                column=0,
                sticky="nsew",
                padx=0,
                pady=0,
            )
            content_l = ttk.Label(
                self.results_tab.scrollbar_frame,
                text=content,
                borderwidth=1,
                relief="ridge",
            )
            content_l.grid(
                row=n,
                column=1,
                sticky="nsew",
                padx=0,
                pady=0,
            )
            progress_l = ttk.Label(
                self.results_tab.scrollbar_frame,
                text=progress,
                borderwidth=1,
                relief="ridge",
            )
            progress_l.grid(
                row=n,
                column=2,
                sticky="nsew",
                padx=0,
                pady=0,
            )
            export_button = ttk.Button(
                self.results_tab.scrollbar_frame,
                text="Экспортировать",
                command=lambda x=query_id: self.export_result(x),  # type: ignore
                bootstyle="outline",
            )
            export_button.grid(row=n, column=3, sticky="nsew", padx=0, pady=0)
            self.displayed_results.extend((created_at_l, content_l, progress_l, export_button))

    def get_fetch_size(self) -> int:
        user_fetch_size = self.fetch_size.get()
        if not user_fetch_size:
            messagebox.showerror("Ошибка", "Заполните лимит записей")
            return -1
        if not self.is_positive_int(user_fetch_size):
            messagebox.showerror(
                "Ошибка", "Лимит записей должен быть положительным целым числом"
            )
            return -1
        return int(user_fetch_size)

    def get_user_entry(self, selected_stands: list[str]) -> str:
        user_entry = self.highlighted_frame.entry.get("1.0", tk.END).strip()
        if not self.highlighted_frame.is_valid_entry(user_entry, selected_stands):
            return ""
        return user_entry

    def create_tasks(self) -> None:
        user_fetch_size = self.get_fetch_size()
        if user_fetch_size < 1:
            return None
        selected_stands = [
            stand.name for stand in stands.values() if stand.checkbox.get() == True
        ]
        if not selected_stands:
            messagebox.showerror(
                "Ошибка", "Выберите адрес(-а) БД перед выполнением запроса"
            )
            return None
        user_query = self.get_user_entry(selected_stands)
        if not user_query:
            return None

        syntax = self.highlighted_frame.highlight_choice.get()
        query_id = self.storage.save_query(syntax, user_query, len(selected_stands))
        for stand_name in selected_stands:
            if q.qsize() < MAX_QUEUE_SIZE:
                q.put(
                    QueryTask(
                        stand_name,
                        query_id,
                        user_query,
                        user_fetch_size,
                    )
                )
            else:
                self.storage.mark_incomplete(query_id)
                messagebox.showinfo(
                    "Инфо", "Очередь заполнена, дождитесь выполнения задач"
                )
                break


class Stand:
    syntax_map = {"sqlite": Syntax.SQL, "postgres": Syntax.SQL, "mongo": Syntax.MONGO}
    __slots__ = ("name", "vendor", "syntax", "db", "checkbox", "checkbutton")

    def __init__(self, name: str, vendor: str, db: DB):
        self.name = name
        self.vendor = vendor
        self.syntax = Stand.syntax_map[vendor]
        self.db = db
        self.checkbox = tk.BooleanVar(value=False)
        self.checkbutton: ttk.Checkbutton

    def __repr__(self):
        return f"Stand(name={self.name!r}, vendor={self.vendor!r}, syntax={self.syntax!r}, db={self.db!r}, checkbox={self.checkbox!r})"


class QueryTask(NamedTuple):
    stand_name: str
    query_id: int
    query: str
    size: int


def execute_query(task: QueryTask) -> None:
    stand = stands[task.stand_name]
    try:
        stand.db.connect()
    except DBError as e:
        result = [{"error": str(e)}]
        is_error = 1
    else:
        try:
            result = stand.db.fetchmany(task.query, task.size)
            is_error = 0
        except DBError as e:
            result = [{"error": str(e)}]
            is_error = 1
    storage = Storage(INTERNAL_DB)
    storage.connect()
    storage.save_result(task.query_id, is_error, task.stand_name, result)
    storage.close()


def worker() -> None:
    while True:
        query_task = q.get()
        execute_query(query_task)


@atexit.register
def shutdown() -> None:
    for stand in stands.values():
        stand.db.close()


try:
    with open("config.json") as f:
        config = json.load(f)
except FileNotFoundError:
    config = {"max_queue_size": 1, "num_workers": 1, "connections": {}}

q: queue.Queue = queue.Queue()
stands: dict[str, Stand] = {}
MAX_QUEUE_SIZE = config.get("max_queue_size", 50)
NUM_WORKERS = config.get("num_workers", 4)
INTERNAL_DB = "internal.db"
THEME = config.get("theme", "pulse")


def main() -> None:
    win = ttk.Window(themename=THEME)
    win.geometry("800x700")
    win.title("Запрос на несколько БД")
    wgui = Wgui(win)
    wgui.storage.connect()
    wgui.storage.setup()
    wgui.draw_stands()
    for _ in range(NUM_WORKERS):
        t = threading.Thread(target=worker, daemon=True)
        t.start()

    win.mainloop()


if __name__ == "__main__":
    main()
