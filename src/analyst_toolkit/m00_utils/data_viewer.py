"""
📦 Module: data_viewer.py

Prototype interactive widget for viewing tabular data (DataFrame or Excel file) in Jupyter notebooks.
Intended for integration into later iterations of the Analyst Toolkit as a reusable data preview utility.
"""

import ipywidgets as widgets
import pandas as pd
from IPython.display import display


class DataViewer:
    """
    An interactive widget for viewing tabular data from a DataFrame or Excel file.

    This viewer supports inline display of pandas DataFrames or multi-sheet Excel files
    via ipywidgets. Excel files automatically generate a dropdown to select sheets.
    """

    def __init__(self, source, title: str = "Data Viewer"):
        """
        Initializes the DataViewer widget.

        Args:
            source (Union[pd.DataFrame, str]): A DataFrame or path to an Excel (.xlsx) file.
            title (str): Optional widget title to display above the table.
        """
        self.source = source
        self.title = title
        self.widget_box = self._build_ui()

    def _build_ui(self):
        """
        Constructs the ipywidgets-based UI for displaying the data.

        Returns:
            VBox: A vertical box containing the title, sheet selector (if applicable), and output area.
        """
        title_widget = widgets.HTML(f"<h4>{self.title}</h4>")

        # Output widget to display the DataFrame table
        output_widget = widgets.Output()

        if isinstance(self.source, pd.DataFrame):
            # No sheet selection needed for a DataFrame; create a disabled dropdown placeholder
            sheet_dropdown = widgets.Dropdown(disabled=True)
            with output_widget:
                display(self.source)
        elif isinstance(self.source, str) and self.source.endswith(".xlsx"):
            # Load Excel file and extract available sheet names
            try:
                xls = pd.ExcelFile(self.source)
                sheet_names = xls.sheet_names
                sheet_dropdown = widgets.Dropdown(
                    options=sheet_names,
                    description="Sheet:",
                    style={"description_width": "initial"},
                )

                def display_sheet(change):
                    """Callback to load and display the selected sheet."""
                    output_widget.clear_output()
                    with output_widget:
                        df = pd.read_excel(xls, sheet_name=change.new)
                        display(df)

                # Bind dropdown selection to the display callback
                sheet_dropdown.observe(display_sheet, names="value")

                # Display the first sheet by default on load
                with output_widget:
                    display(pd.read_excel(xls, sheet_name=sheet_names[0]))

            except FileNotFoundError:
                sheet_dropdown = widgets.Dropdown(disabled=True)
                with output_widget:
                    print(f"Error: File not found at '{self.source}'")
        else:
            raise TypeError("Source must be a pandas DataFrame or a path to an .xlsx file.")

        return widgets.VBox([title_widget, sheet_dropdown, output_widget])

    def display(self):
        """
        Displays the assembled widget in the notebook.
        """
        display(self.widget_box)
