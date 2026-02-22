"""
📦 Module: plot_viewer_comparison.py

Interactive image viewer widget for comparing plot files in Jupyter notebooks.

This module defines an enhanced widget for QA and EDA workflows. It supports:
- Single and side-by-side comparison of image plots
- Automatic image loading from a directory
- Inline rendering with ipywidgets UI elements
"""

import os
from pathlib import Path

import ipywidgets as widgets
from IPython.display import display


class PlotViewerOutliers:
    """
    An interactive widget for browsing and comparing plot images in Jupyter notebooks.

    Supports single-view and comparison mode, and is designed for use in
    exploratory analysis and pipeline QA steps.
    """

    SUPPORTED_FORMATS = (".png", ".jpg", ".jpeg", ".svg")

    # --- THIS IS THE FIX: Sizing Constants Adjusted ---
    SINGLE_PLOT_WIDTH = 700
    SINGLE_PLOT_HEIGHT = 450  # Reduced from 600
    COMPARE_PLOT_WIDTH = 450
    COMPARE_PLOT_HEIGHT = 350  # Reduced from 400
    # --- END OF FIX ---

    def __init__(self, image_dir: str, title: str = None):
        """
        Initializes the PlotViewerOutliers widget and builds the UI.

        Args:
            image_dir (str): Path to a directory containing image files.
            title (str, optional): Optional title displayed above the widget.
        """
        if not os.path.isdir(image_dir):
            raise FileNotFoundError(f"Image directory not found at: {image_dir}")
        self.image_dir = image_dir
        self.title = title
        self.image_files = self._load_image_files()

        self.widget_box = self._build_ui()

    def _load_image_files(self) -> list:
        """
        Finds all supported image files in the directory and returns them sorted.

        Returns:
            list: A list of image filenames (e.g., ['boxplot.png', 'histogram.png']).
        """
        # Filter for supported image formats in the given directory
        files = [
            f for f in os.listdir(self.image_dir) if f.lower().endswith(self.SUPPORTED_FORMATS)
        ]
        files.sort()
        return files

    def _get_image_data(self, filename: str) -> bytes:
        """
        Reads image byte data from a given filename.

        Args:
            filename (str): Name of the image file to load.

        Returns:
            bytes: Raw image data or empty bytes if not found.
        """
        if not filename or filename == "- Select a plot -":
            return b""
        try:
            with open(Path(self.image_dir) / filename, "rb") as f:
                return f.read()
        except Exception as e:
            print(f"Error loading image {filename}: {e}")
            return b""

    def _build_ui(self):
        """
        Constructs the full ipywidgets interface for the viewer.

        Returns:
            VBox: A vertical container with title, dropdowns, and images.
        """
        title_widget = widgets.HTML(
            f"<h3 style='margin-top:10px'>{self.title}</h3>" if self.title else ""
        )
        # Toggle for activating comparison mode
        compare_checkbox = widgets.Checkbox(
            value=False, description="Comparison Mode", indent=False
        )

        plot_options = ["- Select a plot -"] + self.image_files
        dropdown1 = widgets.Dropdown(
            options=plot_options,
            description="Plot 1:",
            style={"description_width": "initial"},
            layout={"width": "400px"},
        )
        dropdown2 = widgets.Dropdown(
            options=plot_options,
            description="Plot 2:",
            style={"description_width": "initial"},
            layout={"width": "400px", "visibility": "hidden"},
        )

        # Use constants to set initial size
        image_widget1 = widgets.Image(
            format="png",
            layout=widgets.Layout(
                width=f"{self.SINGLE_PLOT_WIDTH}px",
                height=f"{self.SINGLE_PLOT_HEIGHT}px",
                object_fit="contain",
            ),
        )
        image_widget2 = widgets.Image(
            format="png",
            layout=widgets.Layout(
                width=f"{self.COMPARE_PLOT_WIDTH}px",
                height=f"{self.COMPARE_PLOT_HEIGHT}px",
                object_fit="contain",
            ),
        )

        images_box = widgets.HBox([image_widget1], layout=widgets.Layout(justify_content="center"))

        # Updates layout and visibility when comparison mode is toggled
        def _toggle_comparison_mode(change):
            is_comparison = change["new"]
            dropdown2.layout.visibility = "visible" if is_comparison else "hidden"
            if is_comparison:
                image_widget1.layout.width, image_widget1.layout.height = (
                    f"{self.COMPARE_PLOT_WIDTH}px",
                    f"{self.COMPARE_PLOT_HEIGHT}px",
                )
                images_box.layout.justify_content = "flex-start"
                images_box.children = (image_widget1, image_widget2)
            else:
                image_widget1.layout.width, image_widget1.layout.height = (
                    f"{self.SINGLE_PLOT_WIDTH}px",
                    f"{self.SINGLE_PLOT_HEIGHT}px",
                )
                dropdown2.value = "- Select a plot -"
                image_widget2.value = b""
                images_box.layout.justify_content = "center"
                images_box.children = (image_widget1,)

        # Loads and updates the selected image preview
        def _update_plot(change):
            new_filename = change.new
            owner = change.owner
            if owner == dropdown1:
                image_widget1.value = self._get_image_data(new_filename)
            elif owner == dropdown2:
                image_widget2.value = self._get_image_data(new_filename)

        compare_checkbox.observe(_toggle_comparison_mode, names="value")
        dropdown1.observe(_update_plot, names="value")
        dropdown2.observe(_update_plot, names="value")

        # Manually trigger initial plot load
        if dropdown1.value:
            image_widget1.value = self._get_image_data(dropdown1.value)

        return widgets.VBox(
            [title_widget, widgets.HBox([dropdown1, dropdown2]), compare_checkbox, images_box]
        )

    def render(self):
        """Renders the assembled widget in the notebook."""
        if not self.image_files:
            display(widgets.HTML("<p><em>PlotViewer: No plot images found to display.</em></p>"))
            return
        display(self.widget_box)
