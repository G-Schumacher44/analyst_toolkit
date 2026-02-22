"""
📦 Module: plot_viewer.py

An interactive widget for browsing individual plot images in Jupyter notebooks.

This viewer allows selection of a category and then a plot within that category
from a dictionary of paths. Designed for simple, clean single-plot display
within data QA or EDA steps.
"""

from pathlib import Path

import ipywidgets as widgets
from IPython.display import display


class PlotViewer:
    """
    Interactive widget for viewing individual plots organized by category.

    This widget does not support comparison mode. It is designed for simple
    preview of static images during analysis and reporting.
    """

    def __init__(self, plot_paths: dict, title: str = None):
        """
        Initializes the viewer with a dictionary of plot paths.

        Args:
            plot_paths (dict): A dictionary where keys are category names (str)
                               and values are lists of plot file paths (Path objects or strings).
            title (str, optional): An optional title for the widget.
        """
        if not isinstance(plot_paths, dict):
            raise TypeError("plot_paths must be a dictionary.")

        self.plot_paths = plot_paths
        self.title = title
        self.widget_box = self._build_ui()

    def _build_ui(self):
        """
        Builds the UI components for the plot viewer.

        Returns:
            VBox: The assembled widget box containing title, dropdowns, and image output.
        """
        # --- Widget Components ---
        self.title_widget = widgets.HTML(
            f"<h3 style='margin-top:10px'>{self.title}</h3>" if self.title else ""
        )

        # Dropdown for selecting the plot category (e.g., Numeric, Categorical)
        self.category_dropdown = widgets.Dropdown(
            options=list(self.plot_paths.keys()),
            description="Category:",
            style={"description_width": "initial"},
            layout={"width": "300px"},
        )

        # Dropdown for selecting the specific plot file within a category
        self.plot_dropdown = widgets.Dropdown(
            description="Plot:", style={"description_width": "initial"}, layout={"width": "500px"}
        )

        # Image widget to display the selected plot
        self.image_output = widgets.Image(
            value=b"",
            format="png",
            layout=widgets.Layout(
                width="95%", height="auto", object_fit="contain", margin="10px 0 0 0"
            ),
        )

        # --- Define Interactions ---
        def update_plot_options(change):
            """Callback to update plot dropdown when category changes."""
            category = change.new
            # Create a user-friendly mapping from filename to full path
            new_options = {Path(p).name: p for p in self.plot_paths.get(category, [])}
            self.plot_dropdown.options = new_options

        def display_plot(change):
            """Callback to display the selected image."""
            plot_path = change.get("new")
            if plot_path and Path(plot_path).exists():
                with open(plot_path, "rb") as f:
                    self.image_output.value = f.read()
            else:
                self.image_output.value = b""

        # Link the callbacks to the dropdowns
        self.category_dropdown.observe(update_plot_options, names="value")
        self.plot_dropdown.observe(display_plot, names="value")

        # Initialize the plot dropdown with the first category's options
        initial_category = next(iter(self.plot_paths.keys()), None)
        if initial_category:
            self.plot_dropdown.options = {
                Path(p).name: p for p in self.plot_paths.get(initial_category, [])
            }
            # Trigger the display of the first plot if options exist
            if self.plot_dropdown.options:
                display_plot({"new": self.plot_dropdown.value})

        # Arrange widgets vertically
        return widgets.VBox(
            [
                self.title_widget,
                widgets.HBox([self.category_dropdown, self.plot_dropdown]),
                widgets.HBox([self.image_output], layout=widgets.Layout(justify_content="center")),
            ]
        )

    def display(self):
        """Renders the assembled widget in the notebook."""
        display(self.widget_box)
